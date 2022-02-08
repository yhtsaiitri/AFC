#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
//#include <iostream>
#include "modbus.h"
//#include "config.h"
#define SERVER_ID   1

//using namespace std;


//libmodbus-3.1.6
int main(void)
{
    int server_socket = -1;
    modbus_t *ctx;
    modbus_mapping_t *mb_mapping;
    ctx = modbus_new_tcp(NULL,502);
    modbus_set_debug(ctx,FALSE);

    //modbus_set_slave(ctx,SERVER_ID);


    mb_mapping = modbus_mapping_new(0,0,500,500);
    if(mb_mapping == NULL)
    {
        fprintf(stderr,"Failed mapping:%s\n",modbus_strerror(errno));
        modbus_free(ctx);
        return -1;
    }
    

    printf("listen\n");
    server_socket = modbus_tcp_listen(ctx,1);
    if(server_socket == -1)
    {
        fprintf(stderr,"Unable to listen TCP.\n");
        modbus_free(ctx);
        return -1;
    }



    modbus_tcp_accept(ctx,&server_socket);

    while(1)
    {
        uint8_t query[MODBUS_TCP_MAX_ADU_LENGTH];
        int rc;

        rc = modbus_receive(ctx,query);
        if(rc >= 0)
        {
            /* rc is the query size */
            //mb_mapping->tab_registers[1] = 2333;
            //mb_mapping->tab_input_registers[1] = 2333;
            /*
            if(mb_mapping->tab_bits == NULL)
            {
                printf("mb_mapping->tab_bits == NULL\n");
            }else{
                printf("mb_mapping->tab_bits == QQQQ\n");
            }
            if(mb_mapping->tab_input_bits == NULL)
            {
                printf("mb_mapping->tab_input_bits == NULL\n");
            }else{
                printf("mb_mapping->tab_input_bits == QQQQ\n");
                //printf("mb_mapping->tab_input_bits == QQQQ  %d\n", mb_mapping->tab_input_bits);
                //mb_mapping->tab_input_bits[0] = 2333;
            }
            */
            /*
            if(mb_mapping->tab_registers == NULL)
            {
                printf("mb_mapping->tab_registers == NULL\n");
            }else{
                printf("mb_mapping->tab_registers == QQQQ\n");
                //mb_mapping->tab_registers[2] = 5682;
            }
            if(mb_mapping->tab_input_registers == NULL)
            {
                printf("mb_mapping->tab_input_registers == NULL\n");
            }else{
                printf("mb_mapping->tab_input_registers == QQQQ\n");
                mb_mapping->tab_input_registers[0] = 2;
            }
            */
            modbus_reply(ctx,query,rc,mb_mapping);
            
            //printf("query : %hhn\n", query);
            //printf("rc : %d\n", rc);
            printf("mb_mapping->tab_registers[0] : %d\n", mb_mapping->tab_registers[0]);
            //printf("mb_mapping->tab_registers[1] : %d\n", mb_mapping->tab_registers[1]);
            //printf("mb_mapping->tab_registers[2] : %d\n", mb_mapping->tab_registers[2]);
            //printf("mb_mapping->tab_registers[3] : %d\n", mb_mapping->tab_registers[3]);
            
            //cout<<mb_mapping->tab_registers[0]<<endl;// 打印第一个保持寄存器
        }
        else
        {
            // Connection closed by the client or error
            printf("Connection Closed.\n");

            //Close ctx
            modbus_close(ctx);


            modbus_tcp_accept(ctx,&server_socket);
        }

    }

    printf("Quit the loop:%s\n",modbus_strerror(errno));

    modbus_mapping_free(mb_mapping);
    modbus_close(ctx);
    modbus_free(ctx);

    return 0;
}