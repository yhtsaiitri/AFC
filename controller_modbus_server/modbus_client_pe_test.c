#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
 
#include <modbus-tcp.h>

//#define MODBUS_SERVER_HOST         "127.0.0.1"
#define MODBUS_SERVER_HOST         "10.172.21.14"
//#define MODBUS_SERVER_HOST         "10.172.21.102"
#define MODBUS_SERVER_PORT          502
#define MODBUS_DEVICE_ID            100
#define MODBUS_TIMEOUT_SEC          3
#define MODBUS_TIMEOUT_USEC         0
//#define MODBUS_DEBUG                ON
#define MODBUS_DEBUG                OFF
 
//#define MODBUS_DISCRETE_ADDR        0
//#define MODBUS_DISCRETE_LEN         32
//#define MODBUS_COIL_ADDR            0
//#define MODBUS_COIL_LEN             32
#define MODUBS_INPUT_ADDR           0
#define MODUBS_INPUT_LEN            90
#define MODUBS_HOLDING_ADDR         0
#define MODUBS_HOLDING_LEN          64

int main(int argc, char *argv[])
{
    //modbus
    modbus_t        *modbus_client;
    struct timeval  timeout;
    uint32_t        timeout_sec;
    uint32_t        timeout_usec;
    int             ret, ii;
    //uint8_t         bits[MODBUS_MAX_READ_BITS] = {0};
    uint16_t        regs[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        write_data[MODBUS_MAX_READ_REGISTERS] = {3};
 

    //pcs
    modbus_client = modbus_new_tcp(MODBUS_SERVER_HOST, MODBUS_SERVER_PORT);
 
    /* set device ID */
    modbus_set_slave(modbus_client, MODBUS_DEVICE_ID);
 
    /* Debug mode */
    modbus_set_debug(modbus_client, MODBUS_DEBUG);
 
    /* set timeout */
    timeout_sec = MODBUS_TIMEOUT_SEC;
    timeout_usec = MODBUS_TIMEOUT_USEC;
    modbus_get_byte_timeout(modbus_client, &timeout_sec, &timeout_usec);
 
    timeout_sec = MODBUS_TIMEOUT_SEC;
    timeout_usec = MODBUS_TIMEOUT_USEC;
    modbus_set_response_timeout(modbus_client, timeout_sec, timeout_usec);
 
    if (modbus_connect(modbus_client) == -1) {
        fprintf(stderr, "Connexion failed: %s\n",
                modbus_strerror(errno));
        modbus_free(modbus_client);
        exit(-1);
    }

    //ret = modbus_read_input_registers(modbus_client, MODUBS_INPUT_ADDR, MODUBS_INPUT_LEN, regs);

    ret = modbus_read_input_registers(modbus_client, 1122, 15, regs);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    } else {
        printf("ret: %d\n", ret);
        printf("HOLDING REGISTERS 1122:\n");
        for (ii=0; ii < ret; ii++) {
            printf("[%d]=%d ", ii, regs[ii]);
        }
        printf("\n");
    }

    write_data[0] = 1000;
    modbus_write_registers(modbus_client, 552, 1, write_data);
    ret = modbus_read_registers(modbus_client, 552, 1, regs);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    } else {
        printf("ret: %d\n", ret);
        printf("HOLDING REGISTERS 552 P:\n");
        for (ii=0; ii < ret; ii++) {
            printf("[%d]=%d ", ii, regs[ii]);
        }
        printf("\n");
    }

    write_data[0] = 500;
    modbus_write_registers(modbus_client, 557, 1, write_data);
    ret = modbus_read_registers(modbus_client, 557, 1, regs);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    } else {
        printf("ret: %d\n", ret);
        printf("HOLDING REGISTERS 557 Q:\n");
        for (ii=0; ii < ret; ii++) {
            printf("[%d]=%d ", ii, regs[ii]);
        }
        printf("\n");
    }

    write_data[0] = 1;
    modbus_write_registers(modbus_client, 2041, 1, write_data);
    ret = modbus_read_registers(modbus_client, 2041, 1, regs);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    } else {
        printf("ret: %d\n", ret);
        printf("HOLDING REGISTERS 2041 Enable main selector:\n");
        for (ii=0; ii < ret; ii++) {
            printf("[%d]=%d ", ii, regs[ii]);
        }
        printf("\n");
    }

    ret = modbus_read_input_registers(modbus_client, 1122, 1, regs);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    } else {
        printf("ret: %d\n", ret);
        printf("HOLDING REGISTERS 1122 Inverter status (read):\n");
        for (ii=0; ii < ret; ii++) {
            printf("[%d]=%d ", ii, regs[ii]);
        }
        printf("\n");
    }

    ret = modbus_read_input_registers(modbus_client, 1007, 2, regs);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    } else {
        printf("ret: %d\n", ret);
        printf("HOLDING REGISTERS 1007 Active Power:\n");
        for (ii=0; ii < ret; ii++) {
            printf("[%d]=%d ", ii, regs[ii]);
        }
        printf("\n");
    }
/*
    //write
    modbus_write_registers(modbus_client, 0, 1, write_data);

    //ret = modbus_read_registers(modbus_client, 0, 1, regs);
    ret = modbus_read_registers(modbus_client, 0, 10, regs);
    //ret = modbus_read_input_registers(modbus_client, 0, 10, regs);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    } else {
        printf("ret: %d\n", ret);
        printf("HOLDING REGISTERS:\n");
        for (ii=0; ii < ret; ii++) {
            printf("[%d]=%d ", ii, regs[ii]);
        }
        printf("\n");
    }

    ret = modbus_read_input_registers(modbus_client, 0, 10, regs);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    } else {
        printf("ret: %d\n", ret);
        printf("INPUT REGISTERS:\n");
        for (ii=0; ii < ret; ii++) {
            printf("[%d]=%d ", ii, regs[ii]);
        }
        printf("\n");
    }
*/
    modbus_close(modbus_client);
    modbus_free(modbus_client);

    return 0;
}