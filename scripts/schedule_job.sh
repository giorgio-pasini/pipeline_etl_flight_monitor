#!/bin/bash
# Job scheduler using cron — Run job every 2 hours
#
# Usage:
#   1. Rendre executable: chmod +x scripts/schedule_job.sh
#   2. Installer cron job: ./scripts/schedule_job.sh install
#   3. Voir jobs: ./scripts/schedule_job.sh list
#   4. Supprimer: ./scripts/schedule_job.sh remove

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOB_SCRIPT="$SCRIPT_DIR/scripts/run_job.py"
LOG_FILE="$SCRIPT_DIR/datalake/_logs/cron_schedule.log"

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

install_cron() {
    echo "Installing cron job (every 2 hours)..."

    # Create cron job: run at 00:00, 02:00, 04:00, ..., 22:00
    CRON_ENTRY="0 0,2,4,6,8,10,12,14,16,18,20,22 * * * cd $SCRIPT_DIR && python scripts/run_job.py --with-silver-gold >> $LOG_FILE 2>&1"

    # Add to crontab
    (crontab -l 2>/dev/null || true; echo "$CRON_ENTRY") | crontab -

    echo "✓ Cron job installed"
    echo "  Schedule: Every 2 hours (00:00, 02:00, 04:00, ...)"
    echo "  Log: $LOG_FILE"
}

list_cron() {
    echo "Current cron jobs:"
    crontab -l 2>/dev/null | grep -i "run_job.py" || echo "No jobs found"
}

remove_cron() {
    echo "Removing cron job..."
    crontab -l 2>/dev/null | grep -v "run_job.py" | crontab - || true
    echo "✓ Cron job removed"
}

test_job() {
    echo "Testing job (1 execution)..."
    cd "$SCRIPT_DIR"
    python scripts/run_job.py --with-silver-gold
}

help() {
    cat << EOF
Job Scheduler — Étape 7

Usage: ./scripts/schedule_job.sh [command]

Commands:
  install   Install cron job (every 2 hours)
  list      List current cron jobs
  remove    Remove cron job
  test      Test job execution (1 run)
  help      Show this help

Examples:
  ./scripts/schedule_job.sh install   # Install scheduler
  ./scripts/schedule_job.sh test      # Test 1 execution
  ./scripts/schedule_job.sh list      # See scheduled jobs
  ./scripts/schedule_job.sh remove    # Stop scheduler

Note: For Windows, use Windows Task Scheduler instead.
See SCHEDULING.md for details.
EOF
}

case "${1:-help}" in
    install)
        install_cron
        ;;
    list)
        list_cron
        ;;
    remove)
        remove_cron
        ;;
    test)
        test_job
        ;;
    help|--help|-h)
        help
        ;;
    *)
        echo "Unknown command: $1"
        help
        exit 1
        ;;
esac
