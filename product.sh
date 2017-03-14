awk 'match($0,/product_id=[0-9]*/){print $2,$3,$4,substr($0,RSTART,RLENGTH)}' 2017-* | awk '{gsub("\t","");print $0.".html"}' >>wp_dup
cat wp_dup | sort | uniq > wp_product_id