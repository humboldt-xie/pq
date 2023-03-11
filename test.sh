go build ./
set -x
ls -l |./pq   -w "c5>1024  order by c1 limit 1,3"| ./pq   -w "c5>1024  order by c1 limit 2,2"

ls -l |./pq -e ' select * from (select * from stdin)'
