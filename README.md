universal-tracker-stats
=======================

Analyze tracker logs into leaderboard and statistics pages.

Quick Start
===========

Requires:

* Python 3.3+
* sqlalchemy 0.9
* arrow
* Jinja 2

Install Python dependencies:

        pip3 install sqlalchemy arrow Jinja2

Import logs and generate pages:

        python3 -m stats --database DATABASE_NAME.db load LOG_FILE.log.gz
        python3 -m stats --database DATABASE_NAME.db analyze
        python3 -m stats --database DATABASE_NAME.db report MY_PAGES_DIR/
