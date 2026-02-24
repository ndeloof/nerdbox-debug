#!/bin/bash
# Cherry-pick GitHub PRs into current branch
#
# Usage: Edit the invocations below, then run this script
#
# cherry_pick_pr <owner/repo> <pr_number>
#
# Duplicate commits are skipped using two complementary checks:
# 1. git cherry (patch-id comparison) detects commits already in HEAD
# 2. In-memory tracking catches cross-PR duplicates where patch-ids
#    may differ due to context changes (e.g. renames)

set -e

# Track commit SHAs picked during this run to catch cross-PR duplicates
# that git cherry may miss due to patch-id context differences.
PICKED_COMMITS=""

is_already_picked() {
    echo "$PICKED_COMMITS" | grep -qF "$1"
}

record_picked() {
    PICKED_COMMITS="${PICKED_COMMITS}${1}
"
}

cherry_pick_pr() {
    local repo=$1
    local pr=$2
    local pr_branch="pr-${repo//\//-}-$pr"

    echo "=================================================="
    echo "Cherry-picking $repo#$pr"
    echo "=================================================="

    # Fetch the PR
    echo "Fetching PR #$pr from $repo..."
    if ! git fetch "git@github.com:$repo.git" "pull/$pr/head:$pr_branch"; then
        echo "Error: Failed to fetch PR #$pr from $repo"
        return 1
    fi

    # Get merge base and commits
    local merge_base=$(git merge-base HEAD "$pr_branch")
    local total=$(git rev-list --count "$merge_base..$pr_branch")

    echo "Found $total commit(s) in PR"
    echo ""
    git --no-pager log --oneline "$merge_base..$pr_branch"
    echo ""

    # Use git cherry to find which commits are already applied (by patch-id).
    # "+" means not yet applied, "-" means already in HEAD.
    # Additionally check our in-memory set for cross-PR duplicates.
    local picked=0
    local skipped=0
    while IFS=' ' read -r status commit; do
        local oneline=$(git log -1 --oneline "$commit")

        if [ "$status" = "-" ] || is_already_picked "$commit"; then
            echo "Skipping (already applied): $oneline"
            skipped=$((skipped + 1))
            continue
        fi

        echo "Cherry-picking: $oneline"
        if ! git cherry-pick --empty=drop "$commit"; then
            echo ""
            echo "Error: Cherry-pick failed for $oneline"
            echo "Resolve conflicts, then run:"
            echo "  git cherry-pick --continue  # or --skip or --abort"
            echo ""
            echo "After resolving, you can re-run this script."
            echo "Already-applied commits will be skipped automatically."
            git branch -D "$pr_branch" 2>/dev/null || true
            exit 1
        fi
        record_picked "$commit"
        picked=$((picked + 1))
    done < <(git cherry HEAD "$pr_branch" "$merge_base")

    echo "✓ Successfully cherry-picked $repo#$pr ($picked picked, $skipped skipped)"
    echo ""

    # Cleanup
    git branch -D "$pr_branch" 2>/dev/null || true
}

echo "Current branch: $(git branch --show-current)"
echo ""

# ============================================================================
# Edit this section to add/remove PRs to cherry-pick
# ============================================================================

# cherry_pick_pr <owner/repo> <pr_number>
cherry_pick_pr containerd/nerdbox 99 # transfer support
cherry_pick_pr containerd/nerdbox 78 # add flag vnet_hdr
cherry_pick_pr containerd/nerdbox 81 # virtiofs support for bind mounts
cherry_pick_pr docker/docker-next-nerdbox 19 # Previously merged commits that need a new PR
cherry_pick_pr docker/docker-next-nerdbox 21 # disable VM debug logs
cherry_pick_pr docker/docker-next-nerdbox 20 # Windows support
cherry_pick_pr docker/docker-next-nerdbox 22 # Add Hyper-V enlightments to kernel
cherry_pick_pr docker/docker-next-nerdbox 25 # Windows container restart
cherry_pick_pr docker/docker-next-nerdbox 26 # Arch suffix for build artifacts
cherry_pick_pr containerd/nerdbox 104 # update containerd to fix stream issue
cherry_pick_pr containerd/nerdbox 105 # ensure parent socket directory exists
