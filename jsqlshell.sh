#/bin/sh

script="$(readlink -n $0)"
dir="$(dirname $script)"

cp="$dir/target/*"
for jar in `ls $dir/*.jar`; do
  cp=$cp:$jar
done

java -cp "$cp" JSQLShell $1
