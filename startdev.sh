#!/bin/bash
export  USE_LOCAL_REGISTRY=true #设置使用本地镜像仓库
msb server start --dev --debug  #启动msb服务，开启调试模式
