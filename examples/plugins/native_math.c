#include "sparrow_plugin_v1.h"
#include <limits.h>
#ifndef SPARROW_TEST_ABI
#define SPARROW_TEST_ABI 1
#endif
#ifndef SPARROW_PLUGIN_FACTOR
#define SPARROW_PLUGIN_FACTOR 2
#endif
uint32_t sparrow_plugin_abi_v1(void){return SPARROW_TEST_ABI;}
int32_t sparrow_plugin_call_v1(uint32_t function,const SparrowInputV1 *input,uint32_t count,
    SparrowOutputV1 *output,uint8_t *buffer,uint64_t capacity){
    *output=(SparrowOutputV1){0};
    if(count!=1)return 1;
    if(function==1){
        if(input[0].tag!=2)return 2;
        int64_t n=(int64_t)input[0].bits;
        if(n>INT64_MAX/SPARROW_PLUGIN_FACTOR||n<INT64_MIN/SPARROW_PLUGIN_FACTOR)return 3;
        output->tag=2;output->bits=(uint64_t)(n*SPARROW_PLUGIN_FACTOR);return 0;
    }
    if(function==2){
        if(input[0].tag!=5||input[0].len>capacity)return 4;
        for(uint64_t i=0;i<input[0].len;i++){
            uint8_t c=input[0].data[i];buffer[i]=(c>='a'&&c<='z')?(uint8_t)(c-'a'+'A'):c;
        }
        output->tag=5;output->len=input[0].len;return 0;
    }
#ifdef SPARROW_TEST_FAILURES
    if(function>=7&&function<=13){
        if(input[0].len>capacity)return 7;
        for(uint64_t i=0;i<input[0].len;i++)buffer[i]=input[0].data[i];
        output->tag=input[0].tag;output->bits=input[0].bits;output->len=input[0].len;return 0;
    }
    if(function==3){output->tag=5;output->len=capacity+1;return 0;}
    if(function==4){output->tag=1;output->bits=3;return 0;}
    if(function==5){output->tag=4;output->bits=UINT64_C(0x7ff0000000000000);return 0;}
    if(function==6)return 99;
#endif
    return 5;
}
