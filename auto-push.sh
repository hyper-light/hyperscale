#!/bin/bash

# Auto-push script - pushes to specified branch every minute
# Usage: ./auto-push.sh <branch-name>

if [ -z "$1" ]; then
    echo "Usage: $0 <branch-name>"
    echo "Example: $0 feature-branch"
    exit 1
fi

BRANCH="$1"

echo "Starting auto-push to branch '$BRANCH' every 60 seconds..."
echo "Press Ctrl+C to stop"

while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

    # Check if there are any changes to commit
    if [ -n "$(git status --porcelain)" ]; then
        echo "[$TIMESTAMP] Changes detected, staging and committing..."
        git add -A
        git commit -m "Auto-commit: $TIMESTAMP"
    fi

    # Push to the specified branch
    echo "[$TIMESTAMP] Pushing to $BRANCH..."
    if git push origin "$BRANCH" 2>&1; then
        echo "[$TIMESTAMP] Push successful"
    else
        echo "[$TIMESTAMP] Push failed"
    fi

    echo "[$TIMESTAMP] Waiting 60 seconds..."
    sleep 60
done
