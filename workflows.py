import parse_scrapy_insta_csv as pj
import yt_main as yt
from export import run_exports_temp
import runscraping as rs

def main2():
    """
    High level flow:
    1.) Instagram
            - Gather users from sources
            - Ensure no duplicates from DB
            - Scrape
            - Format into correct form
            - Export (ensure that total_like_count and num_followers are not 0, otherwise discard)

    2.) Youtube
            Same flow

    More detailed:
    1.) Instagram
        - Sources:
            !.) Search: searching for random words on insta to get users.
            2.)
    """
    pass


"""
Data gathering workflow:
    - Constantly run youtube and instagram user searches. +
    - Once the number of unexported instagram users has reached a certain number, run scrapy with unexported users. - (might just skip this step, but check out https://www.psycopg.org/docs/advanced.html#asynchronous-notifications)
    - (for the above, will probably have to schedule batches, since it throttles after a certain number) 
    - Once (all) scrapy job(s) have finished, then run export flow. - this can be done from automate.py. 
    
Export flow:
    - Run parser on scrapy output, write to `export` folder. 
    - Export unexported videos and channels, write to `export` folder.
    - Run export function. 
"""


def export_workflow():
    rs.main()
    pj.main()
    yt.main()
    run_exports_temp()
    # call from task, schedule at 8 am every day.
    """
        Once scrapy finishes,
    """
    pass


if __name__ == "__main__":
    print("hello")
