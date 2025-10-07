#!/usr/bin/env python3
import os
import json
import argparse
import time
from datetime import datetime, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from github import Github, GithubException, RateLimitExceededException
from dateutil import parser as date_parser

# Common bot usernames to exclude
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

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-date', required=True)
    parser.add_argument('--end-date', required=True)
    parser.add_argument('--repo-filter', default='')
    parser.add_argument('--max-workers', type=int, default=5, 
                        help='Number of parallel workers (default: 5)')
    return parser.parse_args()

def validate_date_range(start_date, end_date):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    if (end - start).days > 30:
        raise ValueError("Maximum range of 30 days allowed")
    
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
    remaining = rate_limit.core.remaining
    reset_time = rate_limit.core.reset
    
    print(f"Rate Limit: {remaining}/{rate_limit.core.limit} requests remaining")
    
    # If less than min_remaining requests remain, wait for reset
    if remaining < min_remaining:
        wait_time = (reset_time - datetime.utcnow()).total_seconds() + 10
        if wait_time > 0:
            print(f"⚠️  Low rate limit! Waiting {int(wait_time/60)} minutes...")
            time.sleep(wait_time)
            print("✅ Rate limit reset. Continuing...")
            # Verify reset
            rate_limit = github_client.get_rate_limit()
            print(f"Rate Limit after reset: {rate_limit.core.remaining}/{rate_limit.core.limit}")

def get_repositories(github_client, repo_filter):
    """Gets list of repositories from the organization (excludes archived)"""
    org_name = os.getenv('GITHUB_ORG', 'your-org')
    org = github_client.get_organization(org_name)
    
    if repo_filter:
        repo_names = [r.strip() for r in repo_filter.split(',')]
        repos = [org.get_repo(name) for name in repo_names]
    else:
        repos = list(org.get_repos())
    
    # Filter out archived repositories
    active_repos = [repo for repo in repos if not repo.archived]
    archived_count = len(repos) - len(active_repos)
    
    if archived_count > 0:
        print(f"ℹ️  Excluded {archived_count} archived repositories")
    
    return active_repos

def process_single_repo(repo, start_date, end_date, github_client):
    """Process a single repository and return commits"""
    repo_commits = defaultdict(dict)
    repo_name = repo.name
    
    try:
        # Check rate limit before processing
        check_rate_limit(github_client, min_remaining=50)
        
        # Fetch commits in the period from ALL branches
        # GitHub API returns commits from all branches by default
        commits = repo.get_commits(since=start_date, until=end_date)
        
        commit_count = 0
        bot_count = 0
        
        for commit in commits:
            # Skip commits without author
            if not commit.author:
                continue
            
            username = commit.author.login
            sha = commit.sha
            
            # Get author info
            author_name = commit.commit.author.name if commit.commit.author else ''
            author_email = commit.commit.author.email if commit.commit.author else ''
            
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
                    'author_name': author_name,
                    'author_email': author_email,
                    'url': commit.html_url,
                    'stats': {
                        'additions': commit.stats.additions if commit.stats else 0,
                        'deletions': commit.stats.deletions if commit.stats else 0,
                        'total': commit.stats.total if commit.stats else 0
                    }
                }
                
                repo_commits[username][sha] = commit_data
                commit_count += 1
        
        status = f"✓ Found {commit_count} unique commits"
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
    repos = get_repositories(github_client, repo_filter)
    
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
    s3_client = boto3.client('s3')
    bucket = os.getenv('S3_BUCKET')
    
    for username, commits in user_commits.items():
        # Group commits by date
        commits_by_date = defaultdict(list)
        
        for commit_data in commits.values():
            commit_date = date_parser.parse(commit_data['date']).date()
            commits_by_date[commit_date].append(commit_data)
        
        # Upload one file per date
        for date, day_commits in commits_by_date.items():
            date_str = date.strftime('%Y-%m-%d')
            s3_key = f"github/commits/{username}/{date_str}/commits.json"
            
            # Check if file already exists for this date
            existing_commits = {}
            try:
                response = s3_client.get_object(Bucket=bucket, Key=s3_key)
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
                Bucket=bucket,
                Key=s3_key,
                Body=json.dumps(commits_list, indent=2),
                ContentType='application/json'
            )
            
            print(f"Uploaded {len(commits_list)} commits for {username} on {date_str}")

def main():
    args = parse_args()
    
    # Validation
    start_date, end_date = validate_date_range(args.start_date, args.end_date)
    
    # GitHub client
    github_token = os.getenv('GITHUB_TOKEN')
    g = Github(github_token, per_page=100)  # Optimize pagination
    
    # Check initial rate limit
    print("="*60)
    check_rate_limit(g, min_remaining=100)
    print("="*60)
    
    # Collect commits
    print(f"\nCollecting commits from {args.start_date} to {args.end_date}")
    print(f"Excluding bot commits: {len(BOT_USERNAMES)} known bots")
    
    user_commits = collect_commits(
        g, 
        start_date, 
        end_date, 
        args.repo_filter,
        max_workers=args.max_workers
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

if __name__ == '__main__':
    main()