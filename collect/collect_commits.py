#!/usr/bin/env python3
import os
import sys
import json
import argparse
import time
import warnings
import urllib3
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from github import Github, GithubException, RateLimitExceededException
from dateutil import parser as date_parser

# -------- Config via ENV --------
GITHUB_API   = os.getenv("GITHUB_API", "https://api.github.com")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_ORG   = os.getenv("GITHUB_ORG")
START_DATE   = os.getenv("START_DATE")             
END_DATE     = os.getenv("END_DATE")             
REPO_FILTER  = os.getenv("REPO_FILTER", "")   
S3_BUCKET    = os.getenv("S3_BUCKET")
S3_PREFIX    = os.getenv("S3_PREFIX", "github/commits")
MAX_WORKERS  = os.getenv("MAX_WORKERS", 5)

UNSAFE_SKIP_TLS_VERIFY = os.getenv("UNSAFE_SKIP_TLS_VERIFY", "false").lower() in ("1", "true", "yes")

BOT_USERNAMES = {
    'dependabot[bot]',
    'dependabot-preview[bot]',
    'renovate[bot]',
    'renovatebot',
    'greenkeeper[bot]',
    'snyk-bot',
    'github-actions[bot]',
    'codecov[bot]',
    'sonarcloud[bot]',
    'mergify[bot]',
    'allcontributors[bot]',
    'imgbot[bot]',
    'dependabot',
    'renovate',
}

# -------- Validações básicas --------
missing = []
for var in ("GITHUB_TOKEN", "GITHUB_ORG", "START_DATE", "END_DATE", "S3_BUCKET", "S3_PREFIX"):
    if not globals().get(var):
        missing.append(var)
if missing:
    print(f"[ERRO] Missing env: {', '.join(missing)}", file=sys.stderr)
    sys.exit(2)

def validate_date_range(start_date, end_date):
    # start = datetime.strptime(start_date, '%Y-%m-%d')
    # end = datetime.strptime(end_date, '%Y-%m-%d')
    # end = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1) - timedelta(seconds=1)
    start = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc) + timedelta(days=1) - timedelta(seconds=1)
        
    if (end - start).days > 10:
        print("::error::Maximum range of 10 days allowed")
        raise ValueError("Maximum range of 10 days allowed")
    
    return start, end

def is_bot_user(username, author_name='', author_email=''):
    """Check if user is a bot based on username, name or email"""
    if not username:
        return True
    
    # Check username
    username_lower = username.lower()
    if username_lower in BOT_USERNAMES or '[bot]' in username_lower:
        return True
    
    # Check author name
    if author_name:
        name_lower = author_name.lower()
        if 'bot' in name_lower or '[bot]' in name_lower:
            return True
    
    # Check email
    if author_email:
        email_lower = author_email.lower()
        if 'noreply' in email_lower or 'bot' in email_lower:
            return True
    
    return False

def check_rate_limit(github_client, min_remaining=100):
    """Checks and waits if necessary when rate limit is low"""
    rate_limit = github_client.get_rate_limit()
    remaining = rate_limit.rate.remaining
    reset_time = rate_limit.rate.reset
    
    print(f"Rate Limit: {remaining}/{rate_limit.rate.limit} requests remaining")
    
    # If less than min_remaining requests remain, wait for reset
    if remaining < min_remaining:
        wait_time = (reset_time - datetime.now(timezone.utc)).total_seconds() + 10
        if wait_time > 0:
            print(f"⚠️  Low rate limit! Waiting {int(wait_time/60)} minutes...")
            time.sleep(wait_time)
            print("✅ Rate limit reset. Continuing...")
# Verify reset
            rate_limit = github_client.get_rate_limit()
            print(f"Rate Limit after reset: {rate_limit.rate.remaining}/{rate_limit.rate.limit}")


def get_repositories(github_client, repo_filter, start_date, end_date):
    """Gets list of repositories from the organization (excludes archived)"""
    org = github_client.get_organization(GITHUB_ORG)
    
    if repo_filter:
        repo_names = [r.strip() for r in repo_filter.split(',')]
        repos = [org.get_repo(name) for name in repo_names]
    else:
        repos = list(org.get_repos())
    
    active_repos = [repo for repo in repos if not repo.archived]
    archived_count = len(repos) - len(active_repos)

    filtered_repos = [
        repo for repo in active_repos
        if repo.pushed_at and repo.pushed_at >= start_date
        and repo.created_at and repo.created_at <= end_date
    ]    
    skipped_count = len(active_repos) - len(filtered_repos)

    if archived_count > 0:
        print(f"ℹ️  Excluded {archived_count} archived repositories")
    if skipped_count > 0:
        print(f"ℹ️  Excluded {skipped_count} repositories with no relevant activity")

    return filtered_repos

def process_single_repo(repo, start_date, end_date, github_client):
    """Process a single repository and return commits"""
    repo_commits = defaultdict(dict)
    repo_name = repo.name
    
    try:
        # Check rate limit before processing
        check_rate_limit(github_client, min_remaining=50)
        
        branches = list(repo.get_branches())
        
        commit_count = 0
        bot_count = 0
        
        for branch in branches:
            try:
                commits = repo.get_commits(sha=branch.name, since=start_date, until=end_date)

                for commit in commits:
                    # Skip commits without author
                    if not commit.author:
                        continue
            
                    username = commit.author.login
                    sha = commit.sha
                    
                    # Get author info
                    author_name = commit.commit.author.name if commit.commit.author else ''
                    author_email = commit.commit.author.email if commit.commit.author else ''

                    committer_login = commit.committer.login if commit.committer else ''
                    committer_email = commit.commit.committer.email if commit.commit.committer else ''
                    committer_name = commit.commit.committer.name if commit.commit.committer else ''

                    # Skip bots
                    if is_bot_user(username, author_name, author_email):
                        bot_count += 1
                        continue
                    
                    # Avoid duplicates (same commit in multiple branches)
                    if sha not in repo_commits[username]:
                        commit_data = {
                            'sha': sha,
                            'message': commit.commit.message,
                            'date': commit.commit.author.date.isoformat(),
                            'repository': repo_name,
                            'author_login': username,
                            'author_name': author_name,
                            'author_email': author_email,
                            'url': commit.html_url
                        }

                        repo_commits[username][sha] = commit_data
                        commit_count += 1

            except GithubException as e:
                print(f"    ⚠️ Error processing branch {branch.name}: {str(e)}")
        
        status = f"✓ Found {commit_count} unique commits across {len(branches)} branches"
        if bot_count > 0:
            status += f" (excluded {bot_count} bot commits)"
        
return {
            'success': True,
            'repo': repo_name,
            'commits': repo_commits,
            'status': status
        }
                
    except RateLimitExceededException:
        return {
            'success': False,
            'repo': repo_name,
            'error': 'rate_limit',
            'status': '⚠️  Rate limit exceeded'
        }
        
    except GithubException as e:
        return {
            'success': False,
            'repo': repo_name,
            'error': 'github_api',
            'status': f'✗ GitHub API error: {str(e)}'
        }
        
    except Exception as e:
        return {
            'success': False,
            'repo': repo_name,
            'error': 'unexpected',
            'status': f'✗ Unexpected error: {str(e)}'
        }

def collect_commits(github_client, start_date, end_date, repo_filter, max_workers=5):
    """Collects commits from all repositories in parallel"""
    repos = get_repositories(github_client, repo_filter, start_date, end_date)
    
    # Structure: {username: {commit_sha: commit_data}}
    user_commits = defaultdict(dict)
    
    total_repos = len(repos)
    print(f"\nTotal repositories to process: {total_repos}")
    print(f"Using {max_workers} parallel workers\n")
    
    processed = 0
    failed_repos = []
    
    # Process repos in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_repo = {
            executor.submit(process_single_repo, repo, start_date, end_date, github_client): repo
            for repo in repos
        }
        
        # Process completed tasks
        for future in as_completed(future_to_repo):
            processed += 1
            result = future.result()

            print(f"[{processed}/{total_repos}] {result['repo']}: {result['status']}")
            
            if result['success']:
                # Merge commits into main structure
                for username, commits in result['commits'].items():
                    for sha, commit_data in commits.items():
                        if sha not in user_commits[username]:
                            user_commits[username][sha] = commit_data
            else:
                failed_repos.append(result)
                
                # If rate limit error, wait and retry
                if result.get('error') == 'rate_limit':
                    print(f"  Retrying {result['repo']} after rate limit reset...")
                    check_rate_limit(github_client, min_remaining=100)
                    
                    # Retry this repo
                    retry_result = process_single_repo(
                        future_to_repo[future], 
                        start_date, 
                        end_date, 
                        github_client
                    )
                    
                    if retry_result['success']:
                        print(f"  ✓ Retry successful for {result['repo']}")
                        for username, commits in retry_result['commits'].items():
                            for sha, commit_data in commits.items():
                                if sha not in user_commits[username]:
                                    user_commits[username][sha] = commit_data
                        failed_repos.remove(result)
    
    # Report failed repos
    if failed_repos:
        print(f"\n⚠️  Failed to process {len(failed_repos)} repositories:")
        for failed in failed_repos:
            print(f"  - {failed['repo']}: {failed['status']}")
    
    return user_commits

def upload_to_s3(user_commits, start_date, end_date):
    """Uploads data to S3 organized by user and date"""
    s3_client = boto3.client("s3", verify=not UNSAFE_SKIP_TLS_VERIFY)

    warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)

    for username, commits in user_commits.items():
        # Group commits by date
        commits_by_date = defaultdict(list)
        
        for commit_data in commits.values():
            commit_date = date_parser.parse(commit_data['date']).date()
            commits_by_date[commit_date].append(commit_data)
        
        # Upload one file per date
        for date, day_commits in commits_by_date.items():
            date_str = date.strftime('%Y-%m-%d')
            s3_key = f"{S3_PREFIX}/{username}/{date_str}/commits.json"
            
            # Check if file already exists for this date
            existing_commits = {}
            try:
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
                existing_commits = {
                    c['sha']: c for c in json.loads(response['Body'].read())
                }
            except s3_client.exceptions.NoSuchKey:
                pass
            
            # Merge with existing commits (avoid duplicates)
            for commit in day_commits:
                existing_commits[commit['sha']] = commit
            
            # Upload updated file
            commits_list = list(existing_commits.values())
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(commits_list, indent=2),
                ContentType='application/json'
            )
            
            print(f"Uploaded {len(commits_list)} commits for {username} on {date_str}")

def main():    
    # Validation
    start_date, end_date = validate_date_range(START_DATE, END_DATE)
    
    # GitHub client
    github_token = os.getenv('GITHUB_TOKEN')
    g = Github(github_token, per_page=100)  # Optimize pagination
    
    # Check initial rate limit
    print("="*60)
    check_rate_limit(g, min_remaining=100)
    print("="*60)
    
    # Collect commits
    print(f"\nCollecting commits from {START_DATE} to {END_DATE}")
    print(f"Excluding bot commits: {len(BOT_USERNAMES)} known bots")
    
    user_commits = collect_commits(
        g, 
        start_date, 
        end_date, 
        REPO_FILTER,
        max_workers=MAX_WORKERS
    )
    
    print("\n" + "="*60)
    print(f"Total developers: {len(user_commits)}")
    for user, commits in sorted(user_commits.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"  {user}: {len(commits)} commits")
    print("="*60)
    
    # Upload to S3
    print("\nUploading to S3...")
    upload_to_s3(user_commits, start_date, end_date)
    
    # Final rate limit
    print("\n" + "="*60)
    check_rate_limit(g)
    print("="*60)
    
    print("\n✅ Collection completed successfully!")

if __name__ == "__main__":
    main()