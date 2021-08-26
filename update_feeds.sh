#!/bin/sh

cd feedgenerator
scrapy crawl general4 -a azure=true -a force=true
scrapy crawl jobintree2 -a board_id=9
scrapy crawl jobintree2 -a board_id=12
scrapy crawl jobintree2 -a board_id=15
../jobscrapers/utils/toazure.py  feeds/vivastreet.txt
../jobscrapers/utils/toazure.py  feeds/jobintree.txt
../jobscrapers/utils/toazure.py  feeds/capital.txt