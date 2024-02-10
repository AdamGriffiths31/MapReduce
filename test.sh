rm -rf tmp
mkdir tmp || exit 1
rm -rf tmpOut
mkdir tmpOut || exit 1
# Rebuild the files
(go build worker.go)
(go build main.go)
(cd plugins && go build -buildmode=plugin wc.go)

echo "Starting..."

./main &
pid=$!

sleep 1

# Start workers
(./worker plugins/wc.so) &
(./worker plugins/wc.so) &
(./worker plugins/wc.so) &

wait $pid

sort mr-out | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi
