#使用shell提取商品数据(正则表达式)
#以京东为例，使用awk命令，仅供参考
#考虑tab时从原始数据里提取的shell脚本
awk 'match($0,/product_id=[0-9]*/){print $2,$3,$4,substr($0,RSTART,RLENGTH)}' 2017-* | awk '{gsub("\t","");print $0.".html"}' >> result1.txt
#未考虑tab时从原始数据里提取的shell脚本
#awk 'match($0,/http:\/\/item.jd.com\/[0-9]*.html/){print $2,$3,substr($0,RSTART,RLENGTH)}' 2017-01-08 > jd_item
# 对数据去重
cat result1.txt | sort | uniq > wp_origin1
