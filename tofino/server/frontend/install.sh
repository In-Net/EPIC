netccl=$(realpath $(dirname $0)/../..)
tmp=$(mktemp -d)
netccl_base=$(basename $netccl)
set -x
cd $netccl/grpc
./gen_rpc_file.sh
mv ./*.py ./*.pyi $netccl/server/frontend/src/py/
rm -r $netccl/server/frontend/build
cp -r $netccl $tmp
cd $tmp/$netccl_base/server/frontend/
pip install .
rm -r $tmp/$netccl_base