#!/usr/bin/python3
# -*- coding: UTF-8 -*-
# run: BUILD_DIR=$(mktemp -d) pip install .

import os
from os import path
from setuptools import setup, find_packages
from torch.utils.cpp_extension import BuildExtension, CUDAExtension
# from torch.utils.cpp_extension import load
# lltm = load(name='lltm', sources=['lltm_cuda.cpp', 'lltm_cuda_kernel.cu'])

# It is strange that relative pathes go wrong for include_dirs
# Use "getcwd() + relative path" instead

os.chdir(path.dirname(path.abspath(__file__)))

netccl_c = CUDAExtension(
    "netccl._C", # name to import
    sources=["src/c/api.cpp", "src/c/daemon.cpp"], 
    include_dirs=[path.abspath("../libs/argparse/include"), path.abspath("../libs/spdlog/include")], 
    extra_compile_args=['-UNDEBUG', '-fvisibility=hidden'], # -O3 -march=native # enable assert() 
    extra_link_args=['-lpthread', '-lnuma']
)

setup(
    name="netccl", # name in pip
    version="0.0.4",
    author=["Yitao Yuan", "Bohan Zhao", "Yongchao He"],
    # author_email="BohanZhaoIIIS@outlook.com",
    description="Collective Communication Library with In-Network Computing",
    # url="https://github.com:ZeBraHack0/inc-torch.git",
    package_dir={"netccl": "src/py"},
    packages=["netccl"],
    ext_modules=[netccl_c],
    cmdclass={"build_ext": BuildExtension},
    python_requires=">=3.6",
    # install_requires=["numpy>=1.18"]
)
