import os
import json
import argparse
from datetime import datetime, timedelta
import boto3
import pandas as pd
from dateutil import parser as date_parser

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-date', required=True)
    parser.add_argument('--end-date', required=True)
    parser.add_argument('--username', default='')
    parser.add_argument('--format', choices=['excel', 'csv'], default='excel')
    return parser.parse_args()

def validate_date_range(start_date, end_date):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    if (end - start).days > 30:
        raise ValueError("Maximum range of 30 days allowed")
    
    return start, end

def get_date_range(start_date, end_date):
    """Generates list of dates between start and end"""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates

def list_s3_users(s3_client, bucket):
    """Lists all available users in S3"""
    prefix = "github/commits/"
    paginator = s3_client.get_paginator('list_objects_v2')
    
    users = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
        for prefix_obj in page.get('CommonPrefixes', []):
            user = prefix_obj['Prefix'].replace(prefix, '').rstrip('/')
            users.add(user)
    
    return list(users)

def fetch_commits_from_s3(s3_client, bucket, username, start_date, end_date):
    """Fetches commits from S3 for a user in the period"""
    all_commits = []
    dates = get_date_range(start_date, end_date)
    
    for date in dates:
        date_str = date.strftime('%Y-%m-%d')
        s3_key = f"github/commits/{username}/{date_str}/commits.json"
        
        try:
            response = s3_client.get_object(Bucket=bucket, Key=s3_key)
            commits = json.loads(response['Body'].read())
            all_commits.extend(commits)
        except s3_client.exceptions.NoSuchKey:
            # No commits on this day
            continue
        except Exception as e:
            print(f"Error reading {s3_key}: {str(e)}")
            continue
    
    return all_commits

def collect_all_commits(start_date, end_date, username_filter=None):
    """Collects all commits from S3"""
    s3_client = boto3.client('s3')
    bucket = os.getenv('S3_BUCKET')
    
    # Define user list
    if username_filter:
        usernames = [username_filter]
    else:
        usernames = list_s3_users(s3_client, bucket)
    
    print(f"Processing {len(usernames)} user(s)...")
    
    all_data = []
    for username in usernames:
        print(f"  Fetching commits from {username}...")
        commits = fetch_commits_from_s3(s3_client, bucket, username, start_date, end_date)
        
        for commit in commits:
            all_data.append({
                'username': username,
                'author_name': commit.get('author_name', ''),
                'author_email': commit.get('author_email', ''),
                'commit_sha': commit['sha'],
                'commit_date': commit['date'],
                'repository': commit['repository'],
                'message': commit['message'],
                'additions': commit['stats']['additions'],
                'deletions': commit['stats']['deletions'],
                'total_changes': commit['stats']['total'],
                'url': commit['url']
            })
    
    return all_data

def generate_summary_stats(df):
    """Generates summary statistics by user"""
    summary = df.groupby('username').agg({
        'commit_sha': 'count',
        'additions': 'sum',
        'deletions': 'sum',
        'total_changes': 'sum',
        'repository': lambda x: len(x.unique())
    }).rename(columns={
        'commit_sha': 'total_commits',
        'repository': 'repositories_count'
    })
    
    summary = summary.reset_index()
    summary = summary.sort_values('total_commits', ascending=False)
    
    return summary

def save_report(data, summary, start_date, end_date, format_type):
    """Saves the report in the specified format"""
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Sort by date and user
    if not df.empty:
        df['commit_date'] = pd.to_datetime(df['commit_date'])
        df = df.sort_values(['username', 'commit_date'], ascending=[True, False])
    
    # File name
    date_range = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
    
    if format_type == 'excel':
        filename = f"report.{date_range}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Summary sheet
            summary.to_excel(writer, sheet_name='Summary', index=False)
            
            # All commits sheet
            df.to_excel(writer, sheet_name='Commits', index=False)
            
            # Adjust column widths
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"\nExcel report generated: {filename}")
        
    else:  # CSV
        filename_summary = f"report.{date_range}.summary.csv"
        filename_details = f"report.{date_range}.details.csv"
        
        summary.to_csv(filename_summary, index=False)
        df.to_csv(filename_details, index=False)
        
        print(f"\nCSV reports generated:")
        print(f"  - {filename_summary}")
        print(f"  - {filename_details}")

def main():
    args = parse_args()
    
    # Validation
    start_date, end_date = validate_date_range(args.start_date, args.end_date)
    
    # Collect data
    print(f"Generating report from {args.start_date} to {args.end_date}")
    if args.username:
        print(f"Filtering by user: {args.username}")
    
    data = collect_all_commits(start_date, end_date, args.username)
    
    if not data:
        print("\nNo commits found in the specified period.")
        return
    
    # Generate summary
    df = pd.DataFrame(data)
    summary = generate_summary_stats(df)
    
    # Display statistics
    print(f"\n{'='*60}")
    print(f"Total commits: {len(data)}")
    print(f"Total developers: {len(summary)}")
    print(f"\nTop 5 developers by commits:")
    print(summary.head().to_string(index=False))
    print(f"{'='*60}\n")
    
    # Save report
    save_report(data, summary, start_date, end_date, args.format)
    print("\n✅ Report generated successfully!")

if __name__ == '__main__':
    main()#!/usr/bin/env python3