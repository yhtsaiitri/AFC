#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
 
#include <modbus-tcp.h>
#include <pthread.h>
#include <signal.h>
#include <librdkafka/rdkafka.h>
#include "common.h"
#include "json.h"
#include "iec61850_client.h"
#include "hal_thread.h"


//#define PCS_SERVER_HOST            "192.168.1.111"
//#define PCS_SERVER_HOST            "10.172.21.13"
//#define BMS_SERVER_HOST            "10.172.21.102"
//#define CONTROLLER_SERVER_HOST     "10.172.21.14"
//#define MODBUS_SERVER_PORT          502
#define MODBUS_DEVICE_ID            100
#define MODBUS_TIMEOUT_SEC          1
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

#define KAFKA_CONFIG_FILE           "librdkafka.config"
#define KAFKA_PCS_TOPIC             "pcs"
#define KAFKA_BMS_TOPIC             "bms"
#define KAFKA_METER_TOPIC           "meter"
#define KAFKA_SOC_TOPIC             "soc"
#define KAFKA_STATUS_TOPIC          "status"
#define KAFKA_AFC_TOPIC             "run_afc_flag"

//#define IEC61850_SERVER_HOST        "10.172.22.1"
//#define IEC61850_SERVER_PORT        102

//kafka
static rd_kafka_t *rk_pcs;
static rd_kafka_t *rk_bms;
static rd_kafka_t *rk_meter;
static rd_kafka_conf_t *conf_pcs;
static rd_kafka_conf_t *conf_bms;
static rd_kafka_conf_t *conf_meter;
//modbus
static modbus_t        *modbus_client_pcs;
static modbus_t        *modbus_client_bms;
static modbus_t        *modbus_client_controller;
//iec61850
static IedClientError error_meter;
static IedConnection con_meter;

struct thread_para
{
    modbus_t        *modbus_client;
    modbus_t        *modbus_client_controller;
    rd_kafka_t      *rk;
    rd_kafka_conf_t *conf;
};

struct upload_data
{
    int run_afc_flag;
    int controller_status;
    double freq_base;
    double compensate;
    double power_output;
    double power_original;
    double execution_capacity;
    double pcs_nominal_power;
};
typedef struct upload_data upload_data;

struct config_file_data
{
    char pcs_ip[50];
    int pcs_port;
    char bms_ip[50];
    int bms_port;
    char controller_modbus_server_ip[50];
    int controller_modbus_server_port;
    char meter_ip[50];
    int meter_port;
    int default_power_compensate;
};
typedef struct config_file_data config_file_data;

char * iso8601(char local_time[32])
{
  time_t t_time;
  struct tm *t_tm;

  t_time = time(NULL);
  t_tm = localtime(&t_time);
  if (t_tm == NULL)
    return NULL;

  if(strftime(local_time, 32, "%FT%T", t_tm) == 0)
    return NULL;
                
  local_time[25] = local_time[24];
  local_time[24] = local_time[23];
  local_time[22] = ':';
  local_time[26] = '\0';
  return local_time;
}

char * iso8601_us(char local_time[50])
{
    time_t timep; 
    time (&timep); 
    struct tm tm = *localtime(&timep);
    struct timeval time_us;

    gettimeofday(&time_us, NULL);

    sprintf(local_time, "%d-%02d-%02dT%02d:%02d:%02d.%03d", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, time_us.tv_usec);

    return local_time;
}

const char* log_filename(char period[], char log_type[])
{
    static char filename[100] = "";
    time_t timep; 
    time (&timep); 
    struct tm tm = *localtime(&timep);

    memset(filename, 0, sizeof(filename));
    sprintf(filename, "log/%s%s%d-%02d-%02d.txt", period, log_type, tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);

    return filename;
}

void log_received_modbus_data(char period[10], char get_data_time[50], int execution_time, char err[50])
{
    char filename[100] = "";
    char log_type[] = "/recv_modbus_log_";
    char log_data[1000];
    char time[50] = {0};

    strcat(filename, log_filename(period, log_type));
    snprintf(log_data, sizeof(log_data), "execution_time: %d us || now_time: %s || get_data_time: %s || err:  %s\n", execution_time, iso8601_us(time), get_data_time, err);

    FILE* f = fopen(filename, "a");
    fwrite(log_data ,1 ,strlen(log_data), f);

    fclose(f);
}

void log_insert_to_mongodb(char period[10], char get_data_time[50], int execution_time, char err[50])
{
    char filename[100] = "";
    char log_type[] = "/insertDB_log_";
    char log_data[200] = "";
    char time[50] = {0};

    strcat(filename, log_filename(period, log_type));
    snprintf(log_data, sizeof(log_data), "execution_time: %d us || now_time: %s || get_data_time: %s || err:  %s\n", execution_time, iso8601_us(time), get_data_time, err);

    FILE* f = fopen(filename, "a");
    fwrite(log_data ,1 ,strlen(log_data), f);

    fclose(f);
}

void log_total_running_time(char period[10], char start_time[50], int execution_time, int sleep_time)
{
    char filename[100] = "";
    char log_type[] = "/running_time_";
    char log_data[200] = "";

    strcat(filename, log_filename(period, log_type));
    sprintf(log_data, "execution_time: %d us || start_time: %s || sleep_time: %d\n", execution_time, start_time, sleep_time);

    FILE* f = fopen(filename, "a");
    fwrite(log_data ,1 ,strlen(log_data), f);

    fclose(f);
}

static void dr_cb (rd_kafka_t *rk,
                   const rd_kafka_message_t *rkmessage, void *opaque) {
    int *delivery_counterp = (int *)rkmessage->_private; /* V_OPAQUE */

    if (rkmessage->err) {
            fprintf(stderr, "Delivery failed for message %.*s: %s\n",
                    (int)rkmessage->len, (const char *)rkmessage->payload,
                    rd_kafka_err2str(rkmessage->err));
    } else {
/*
            fprintf(stderr,
                    "Message delivered to %s [%d] at offset %"PRId64
                    " in %.2fms: %.*s\n",
                    rd_kafka_topic_name(rkmessage->rkt),
                    (int)rkmessage->partition,
                    rkmessage->offset,
                    (float)rd_kafka_message_latency(rkmessage) / 1000.0,
                    (int)rkmessage->len, (const char *)rkmessage->payload);
*/
            (*delivery_counterp)++;
    }
}


/**
 * @brief Create producer and produce JSON messages.
 *
 * Assumes ownership of \p conf.
 *
 * @returns 0 on success or -1 on error.
 */
static int run_producer (rd_kafka_t *rk, const char *topic, int msgcnt, char data_json[100000]) {

    int i;
    int delivery_counter = 0;
    //char time[32] = {0};

    /* Produce messages */
    for (i = 0 ; run && i < msgcnt ; i++) {
            const char *user = "alice";
            char json[500];
            rd_kafka_resp_err_t err;

/*
            fprintf(stderr, "Producing message #%d to %s: %s=%s\n",
                    i, topic, user, data_json);
*/
            //printf("data_json : %s \n", data_json);

            /* Asynchronous produce */
            err = rd_kafka_producev(
                    rk,
                    RD_KAFKA_V_TOPIC(topic),
                    //RD_KAFKA_V_KEY(user, strlen(user)),
                    RD_KAFKA_V_VALUE(data_json, strlen(data_json)),
                    /* producev() will make a copy of the message
                     * value (the key is always copied), so we
                     * can reuse the same json buffer on the
                     * next iteration. */
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_OPAQUE(&delivery_counter),
                    RD_KAFKA_V_END);
            if (err) {
                    fprintf(stderr, "Produce failed: %s\n",
                            rd_kafka_err2str(err));
                    break;
            }
            /* Poll for delivery report callbacks to know the final
             * delivery status of previously produced messages. */
            rd_kafka_poll(rk, 0);
    }
    if (run) {
            /* Wait for outstanding messages to be delivered,
             * unless user is terminating the application. */
      /*
            fprintf(stderr, "Waiting for %d more delivery results\n",
                    msgcnt - delivery_counter);
      */
            rd_kafka_flush(rk, 15*1000);
    }

    return 0;
}

double iec61850_get(char *get_data_time, config_file_data load_config_file_data)
{
    struct          timeval stop, start;
    char time[50] = {0};
    char *err = "";
    int execution_time = 0;
    double fq = 0;
    double v1 = 0;
    double v2 = 0;
    double v3 = 0;
    double a1 = 0;
    double a2 = 0;
    double a3 = 0;
    double totw = 0;
    double totvar = 0;
    double totpf = 0;
    char log_cmd[128] = "";
    char data_json[5000] = "";

    //printf("default_power_compensate_iec61850_get:%d\n", load_config_file_data.default_power_compensate);

    snprintf(log_cmd, sizeof(log_cmd), "echo time_data_%s_jsonlen_%d > log/meter/temp_log_meter.txt", iso8601_us(time), strlen(data_json));
    system(log_cmd);

    gettimeofday(&start, NULL);

    if (error_meter == IED_ERROR_OK) {

        /* read frequency value from server*/
        MmsValue* frequency = IedConnection_readObject(con_meter, &error_meter, "AXM_WEB2LD0/MMXU1.Hz.instMag.f", IEC61850_FC_MX);

        if (frequency != NULL) {

            if (MmsValue_getType(frequency) == MMS_FLOAT) {
                fq = MmsValue_toFloat(frequency);
                printf("MMXU.Hz.mag.f: %f\n", fq);
            }
            else if (MmsValue_getType(frequency) == MMS_DATA_ACCESS_ERROR) {
                printf("Failed to read frequency (error code: %i)\n", MmsValue_getDataAccessError(frequency));
            }

            MmsValue_delete(frequency);
        }

       
        /* read phsA volt value from server*/
        MmsValue* phsA_V = IedConnection_readObject(con_meter, &error_meter, "AXM_WEB2LD0/MMXU1.PhV.phsA.cVal.mag.f", IEC61850_FC_MX);

        if (phsA_V != NULL) {

            if (MmsValue_getType(phsA_V) == MMS_FLOAT) {
                v1 = MmsValue_toFloat(phsA_V);
                printf("MMXU.PhV.phsA.cVal.mag.f: %f\n", v1);
            }
            else if (MmsValue_getType(phsA_V) == MMS_DATA_ACCESS_ERROR) {
                printf("Failed to read phsA_V (error code: %i)\n", MmsValue_getDataAccessError(phsA_V));
            }

            MmsValue_delete(phsA_V);
        }

        /* read phsB volt value from server*/ 
        MmsValue* phsB_V = IedConnection_readObject(con_meter, &error_meter, "AXM_WEB2LD0/MMXU1.PhV.phsB.cVal.mag.f", IEC61850_FC_MX);

        if (phsB_V != NULL) {

            if (MmsValue_getType(phsB_V) == MMS_FLOAT) {
                v2 = MmsValue_toFloat(phsB_V);
                printf("MMXU.PhV.phsB.cVal.mag.f: %f\n", v2);
            }
            else if (MmsValue_getType(phsB_V) == MMS_DATA_ACCESS_ERROR) {
                printf("Failed to read phsB_V (error code: %i)\n", MmsValue_getDataAccessError(phsB_V));
            }

            MmsValue_delete(phsB_V);
        }

        /* read phsC volt value from server*/ 
        MmsValue* phsC_V = IedConnection_readObject(con_meter, &error_meter, "AXM_WEB2LD0/MMXU1.PhV.phsC.cVal.mag.f", IEC61850_FC_MX);

        if (phsC_V != NULL) {

            if (MmsValue_getType(phsC_V) == MMS_FLOAT) {
                v3 = MmsValue_toFloat(phsC_V);
                printf("MMXU.PhV.phsC.cVal.mag.f: %f\n", v3);
            }
            else if (MmsValue_getType(phsC_V) == MMS_DATA_ACCESS_ERROR) {
                printf("Failed to read phsC_V (error code: %i)\n", MmsValue_getDataAccessError(phsC_V));
            }

            MmsValue_delete(phsC_V);
        }

        /* read phsA A value from server*/    
        MmsValue* phsA_A = IedConnection_readObject(con_meter, &error_meter, "AXM_WEB2LD0/MMXU1.A.phsA.cVal.mag.f", IEC61850_FC_MX);

        if (phsA_A != NULL) {

            if (MmsValue_getType(phsA_A) == MMS_FLOAT) {
                a1 = MmsValue_toFloat(phsA_A);
                printf("MMXU.A.phsA.cVal.mag.f: %f\n", a1);
            }
            else if (MmsValue_getType(phsA_A) == MMS_DATA_ACCESS_ERROR) {
                printf("Failed to read phsA_A (error code: %i)\n", MmsValue_getDataAccessError(phsA_A));
            }

            MmsValue_delete(phsA_A);
        }

        /* read phsB A value from server*/  
        MmsValue* phsB_A = IedConnection_readObject(con_meter, &error_meter, "AXM_WEB2LD0/MMXU1.A.phsB.cVal.mag.f", IEC61850_FC_MX);

        if (phsB_V != NULL) {

            if (MmsValue_getType(phsB_A) == MMS_FLOAT) {
                a2 = MmsValue_toFloat(phsB_A);
                printf("MMXU.A.phsB.cVal.mag.f: %f\n", a2);
            }
            else if (MmsValue_getType(phsB_A) == MMS_DATA_ACCESS_ERROR) {
                printf("Failed to read phsB_A (error code: %i)\n", MmsValue_getDataAccessError(phsB_A));
            }

            MmsValue_delete(phsB_A);
        }

        /* read phsC A value from server*/  
        MmsValue* phsC_A = IedConnection_readObject(con_meter, &error_meter, "AXM_WEB2LD0/MMXU1.A.phsC.cVal.mag.f", IEC61850_FC_MX);

        if (phsC_A != NULL) {

            if (MmsValue_getType(phsC_A) == MMS_FLOAT) {
                a3 = MmsValue_toFloat(phsC_A);
                printf("MMXU.A.phsC.cVal.mag.f: %f\n", a3);
            }
            else if (MmsValue_getType(phsC_A) == MMS_DATA_ACCESS_ERROR) {
                printf("Failed to read phsC_A (error code: %i)\n", MmsValue_getDataAccessError(phsC_A));
            }

            MmsValue_delete(phsC_A);
        }
  
        /* read Total Watt value from server*/  
        MmsValue* TotW = IedConnection_readObject(con_meter, &error_meter, "AXM_WEB2LD0/MMXU1.TotW.mag.f", IEC61850_FC_MX);

        if (TotW != NULL) {

            if (MmsValue_getType(TotW) == MMS_FLOAT) {
                totw = MmsValue_toFloat(TotW);
                printf("MMXU.TotW.mag.f: %f\n", totw);
            }
            else if (MmsValue_getType(TotW) == MMS_DATA_ACCESS_ERROR) {
                printf("Failed to read TotW (error code: %i)\n", MmsValue_getDataAccessError(TotW));
            }

            MmsValue_delete(TotW);
        }
  
        /* read Total VAr value from server*/ 
        MmsValue* TotVAr = IedConnection_readObject(con_meter, &error_meter, "AXM_WEB2LD0/MMXU1.TotVAr.mag.f", IEC61850_FC_MX);

        if (TotVAr != NULL) {

            if (MmsValue_getType(TotVAr) == MMS_FLOAT) {
                totvar = MmsValue_toFloat(TotVAr);
                printf("MMXU.TotVAr.mag.f: %f\n", totvar);
            }
            else if (MmsValue_getType(TotVAr) == MMS_DATA_ACCESS_ERROR) {
                printf("Failed to read TotVAr (error code: %i)\n", MmsValue_getDataAccessError(TotVAr));
            }

            MmsValue_delete(TotVAr);
        }
  
        /* read Total PF value from server*/
        MmsValue* TotPF = IedConnection_readObject(con_meter, &error_meter, "AXM_WEB2LD0/MMXU1.TotPF.mag.f", IEC61850_FC_MX);

        if (TotPF != NULL) {

            if (MmsValue_getType(TotPF) == MMS_FLOAT) {
                totpf = MmsValue_toFloat(TotPF);
                printf("MMXU.TotPF.mag.f: %f\n", totpf);
            }
            else if (MmsValue_getType(TotPF) == MMS_DATA_ACCESS_ERROR) {
                printf("Failed to read TotPF (error code: %i)\n", MmsValue_getDataAccessError(TotPF));
            }

            MmsValue_delete(TotPF);
        }


    }
    else {
        printf("Failed to connect to IEC61850_SERVER_HOST\n");
        IedConnection_close(con_meter);
        IedConnection_destroy(con_meter);
        con_meter = IedConnection_create();
        IedConnection_connect(con_meter, &error_meter, load_config_file_data.meter_ip, load_config_file_data.meter_port);
    }



    gettimeofday(&stop, NULL);
    execution_time = (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec;
    //printf("61850 took %u us\n", execution_time);
    log_received_modbus_data("meter", get_data_time, (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec, err);


    gettimeofday(&start, NULL);
    //insert to kafka broker
    //topic = "pcs";
    //config_file = "librdkafka.config";

    //meter iec61850
    snprintf(data_json, sizeof(data_json),
             "{\"TimeStamp\":\"%s\","
              "\"Frequency\":%f,"
              "\"Total_Watt\":%f,"
              "\"Total_VAr\":%f,"
              "\"Total_PF\":%f,"
              "\"Grid_RS_Voltage\":%f,"
              "\"Grid_ST_Voltage\":%f,"
              "\"Grid_TR_Voltage\":%f,"
              "\"Total_Current_R\":%f,"
              "\"Total_Current_S\":%f,"
              "\"Total_Current_T\":%f}",
              get_data_time, fq, totw, totvar, totpf, v1, v2, v3, a1, a2, a3);

    printf("meter_data: %s\n", data_json);
    snprintf(log_cmd, sizeof(log_cmd), "echo time_data_%s_jsonlen_%d >> log/meter/temp_log_meter.txt", iso8601_us(time), strlen(data_json));
    system(log_cmd);

    if (!(conf_meter = read_config(KAFKA_CONFIG_FILE)))
        printf("KAFKA_CONFIG_FILE error!");
    if (run_producer(rk_meter, KAFKA_METER_TOPIC, 1, data_json) == -1)
    {
        printf("run_producer error!");
        err = "run_producer error!";
    }

    gettimeofday(&stop, NULL);

    log_insert_to_mongodb("meter", get_data_time, (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec, err);

    return fq;
}


/*
       100  A
            .
            .
       perc .   .   .   B
            .           .
          9 .   .   .   .   .   .   C       E
            .           .
         -9 .           .           D       F
            .           .           .       .
      -perc .   .   .   .   .   .   .   .   .   .   .   G
            .           .           .       .           .
            .           .           .       .           .
       -100 .   .   .   .   .   .   .   .   .   .   .   .   .   .   H
            .           .           .       .           .           .
            .           .           .       .           .           .
          FreqA       FreqB       59.98   60.02       FreqG       FreqH

Section 1   |     2     |     3     |   4   |     5     |      6    |     7
*/
double linear_function(double Freq_now, double Freq_1, double Freq_2, int Power_1, int Power_2)
{
    double power_output = 0;
    //功率 = (頻率 - Freq_1) * (Power_1 – Power_2) / (Freq_1 – Freq_2) + Power_1
    power_output = (((Freq_now - Freq_1) * (Power_1 - Power_2)) / (Freq_1 - Freq_2)) + Power_1;
    return power_output;
}

double freq_pq(double freq_base, double Freq_now, int soc)
{
    //dReg 0.25
    double FreqA = freq_base - 0.25;
    double FreqB = freq_base - 0.14;
    double FreqC_D = freq_base - 0.02;
    double FreqE_F = freq_base + 0.02;
    double FreqG = freq_base + 0.14;
    double FreqH = freq_base + 0.25;
    int PowerA = 100;
    int PowerB = 52;
    int PowerC_E = 9;
    int PowerD_F = -9;
    int PowerG = -52;
    int PowerH = -100;
    double power_output = 0;


    if(Freq_now == 0) {
        power_output = 0;
    } else if ((FreqC_D <= Freq_now) && (Freq_now <= FreqE_F)) {      //Section 4
        if(soc > 750) {
            power_output = PowerC_E;
        } else {
            power_output = PowerD_F;
        }
    } else if(Freq_now < FreqC_D) {
        if(Freq_now <= FreqB) {
            if(Freq_now <= FreqA) {           //Section 1
                if(soc > 50) {
                    power_output = PowerA;
                } else {
                    power_output = 0;
                }
            } else {                          //Section 2
                if(soc > 50) {
                    power_output = linear_function(Freq_now, FreqA, FreqB, PowerA, PowerB);
                } else {
                    power_output = 0;
                }
            }
        } else {                              //Section 3
            if(soc > 750) {
                power_output = linear_function(Freq_now, FreqB, FreqC_D, PowerB, PowerC_E);
            } else {
                power_output = linear_function(Freq_now, FreqB, FreqC_D, PowerB, PowerD_F);
            }
        }
    } else if (Freq_now <= FreqG) {           //Section 5
        if(soc > 750) {
            power_output = linear_function(Freq_now, FreqE_F, FreqG, PowerC_E, PowerG);
        } else {
            power_output = linear_function(Freq_now, FreqE_F, FreqG, PowerD_F, PowerG);
        }
    }
    else if (Freq_now <= FreqH) {             //Section 6
        //power_output = linear_function(Freq_now, FreqG, FreqH, PowerG, PowerH);
        if(soc < 950) {
            power_output = linear_function(Freq_now, FreqG, FreqH, PowerG, PowerH);
        } else {
            power_output = 0;
        }
    }
    else{                                     //Section 7
        //power_output = PowerH;
        if(soc < 950) {
            power_output = PowerH;
        } else {
            power_output = 0;
        }
    }

/*
    //Section 1
    if(Freq_now <= FreqA) {
        //power_output = PowerA;
        if(soc > 50) {
            power_output = PowerA;
        } else {
            power_output = 0;
        }
    }
    //Section 2
    else if (Freq_now <= FreqB) {
        //power_output = linear_function(Freq_now, FreqA, FreqB, PowerA, PowerB);
        if(soc > 50) {
            power_output = linear_function(Freq_now, FreqA, FreqB, PowerA, PowerB);
        } else {
            power_output = 0;
        }
    }
    //Section 3
    else if (Freq_now <= FreqC_D) {
        if(soc > 750) {
            power_output = linear_function(Freq_now, FreqB, FreqC_D, PowerB, PowerC_E);
        } else {
            power_output = linear_function(Freq_now, FreqB, FreqC_D, PowerB, PowerD_F);
        }
    }
    //Section 4
    else if (Freq_now <= FreqE_F) {
        if(soc > 750) {
            power_output = PowerC_E;
        } else {
            power_output = PowerD_F;
        }
    }
    //Section 5
    else if (Freq_now <= FreqG) {
        if(soc > 750) {
            power_output = linear_function(Freq_now, FreqE_F, FreqG, PowerC_E, PowerG);
        } else {
            power_output = linear_function(Freq_now, FreqE_F, FreqG, PowerD_F, PowerG);
        }
    }
    //Section 6
    else if (Freq_now <= FreqH) {
        //power_output = linear_function(Freq_now, FreqG, FreqH, PowerG, PowerH);
        if(soc < 950) {
            power_output = linear_function(Freq_now, FreqG, FreqH, PowerG, PowerH);
        } else {
            power_output = 0;
        }
    }
    //Section 7
    else{
        //power_output = PowerH;
        if(soc < 950) {
            power_output = PowerH;
        } else {
            power_output = 0;
        }
    }
*/
    printf("*****************************************************\n");
    printf("soc: %d\n", soc);
    printf("freq_base: %f\n", freq_base);
    printf("Freq_now: %f\n", Freq_now);
    printf("power_output: %f\n", power_output);
    printf("*****************************************************\n");
    return power_output;
}

double execution_capacity_power_percentage(double execution_capacity, double pcs_nominal_power, double power_factor)
{
    double capacity_power_percentage = 0;
    capacity_power_percentage = execution_capacity / (pcs_nominal_power * power_factor);
    return capacity_power_percentage;
}

upload_data power_compensate(double freq_base, double Freq_now, double compensate, int soc, double power_factor, upload_data upload_data_pcs)
{
    //double power_output = 0;
    //double power_original = 0;
    double capacity_power_percentage = 0;
    capacity_power_percentage = execution_capacity_power_percentage(upload_data_pcs.execution_capacity, upload_data_pcs.pcs_nominal_power, power_factor);
    upload_data_pcs.power_original = freq_pq(freq_base, Freq_now, soc);
    upload_data_pcs.power_output = upload_data_pcs.power_original * capacity_power_percentage * compensate;
    if (upload_data_pcs.power_output > 100.0)
        upload_data_pcs.power_output =  100.0;
    return upload_data_pcs;
}

//void get_and_store_pcs_data(modbus_t *modbus_client_pcs, rd_kafka_t *rk, rd_kafka_conf_t *conf, char *get_data_time)
void get_and_store_pcs_data(char *get_data_time, upload_data upload_data_pcs, int heartbeat, config_file_data load_config_file_data)
{
    int             ret, ii;
    uint16_t        modbus_read_pcs_data[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        modbus_read_pcs_status[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        regs[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        write_data[MODBUS_MAX_READ_REGISTERS] = {0};
    struct          timeval stop, start;
    char time[50] = {0};
    //char *get_data_time;
    char *err = "";
    char log_cmd[128] = "";

    char data_json[5000] = "";
    char run_afc_flag_json[100] = "";
    char status_json[5000] = "";


    snprintf(log_cmd, sizeof(log_cmd), "echo time_data_%s_jsonlen_%d > log/p1s/temp_log_p1s.txt", iso8601_us(time), strlen(data_json));
    system(log_cmd);

    gettimeofday(&start, NULL);

    ret = modbus_read_input_registers(modbus_client_pcs, 41008, 50, modbus_read_pcs_data);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
        err = (char *)stderr;
        modbus_client_pcs = modbus_new_tcp(load_config_file_data.pcs_ip, load_config_file_data.pcs_port);
    } else {
        //printf("modbus_read_pcs_data: %d\n", modbus_read_pcs_data[1]);
    }

    ret = modbus_read_input_registers(modbus_client_pcs, 41031, 5, modbus_read_pcs_data);
    printf("@@@@@@@@@@@@@@pcs_data1: %d\n", modbus_read_pcs_data[0]);

    ret = modbus_read_input_registers(modbus_client_pcs, 41000, 50, modbus_read_pcs_data);
    ret = modbus_read_input_registers(modbus_client_pcs, 41121, 5, modbus_read_pcs_status);
    //ret = modbus_read_registers(modbus_client_pcs, 0, 1, regs);
    //iso8601_us(get_data_time);

    printf("@@@@@@@@@@@@@@pcs_data2: %d\n", modbus_read_pcs_data[30]);
    printf("@@@@@@@@@@@@@@pcs_data3: %d\n", modbus_read_pcs_data[31]);
    printf("@@@@@@@@@@@@@@pcs_data4: %d\n", modbus_read_pcs_data[32]);

    write_data[0] = heartbeat;
    modbus_write_registers(modbus_client_pcs, 44252, 1, write_data);


    gettimeofday(&stop, NULL);

    log_received_modbus_data("p1s", get_data_time, (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec, err);


    gettimeofday(&start, NULL);
    //insert to kafka broker
    //topic = "pcs";
    //config_file = "librdkafka.config";

//pe pcs
    snprintf(data_json, sizeof(data_json),
             "{\"TimeStamp\":\"%s\","
              "\"Run_AFC_Flag\":%d,"
              "\"Controller_Status\":%d,"
              "\"Frequency_Base\":%f,"
              "\"Power_Compensate\":%f,"

              "\"Power_Output\":%f,"
              "\"Power_Original\":%f,"
              "\"Execution_Capacity\":%f,"
              "\"PCS_Nominal_Power\":%f,"
              "\"Heartbeat\":%d,"

              "\"Grid_Active_Power\":%d,"   //41008 kw
              "\"Grid_Reactive_Power\":%d," //41009 kvar
              "\"Power_Factor\":%d,"        //41011 /1000
              "\"Current_fault\":%d,"       //41121 0 = no fault
              "\"Current_warning\":%d,"     //41122 0 = no warning

              "\"Current_status\":%d,"      //41123 9 = fault
              "\"Grid_RS_Voltage\":%d,"     //41031 V
              "\"Grid_ST_Voltage\":%d,"     //41032 V
              "\"Grid_TR_Voltage\":%d,"     //41033 V
              "\"Total_Current_R\":%d,"     //41034 A

              "\"Total_Current_S\":%d,"     //41035 A
              "\"Total_Current_T\":%d}",    //41036 A

              get_data_time, 
              upload_data_pcs.run_afc_flag, 
              upload_data_pcs.controller_status, 
              upload_data_pcs.freq_base,
              upload_data_pcs.compensate, 

              upload_data_pcs.power_output, 
              upload_data_pcs.power_original, 
              upload_data_pcs.execution_capacity, 
              upload_data_pcs.pcs_nominal_power, 
              heartbeat,

              modbus_read_pcs_data[8], 
              modbus_read_pcs_data[9], 
              modbus_read_pcs_data[11], 
              modbus_read_pcs_status[0], 
              modbus_read_pcs_status[1], 

              modbus_read_pcs_status[2], 
              modbus_read_pcs_data[31],
              modbus_read_pcs_data[32], 
              modbus_read_pcs_data[33], 
              modbus_read_pcs_data[34], 

              modbus_read_pcs_data[35],
              modbus_read_pcs_data[36]);

    snprintf(run_afc_flag_json, sizeof(run_afc_flag_json),
             "{\"TimeStamp\":\"%s\","
              "\"Run_AFC_Flag\":%d}",
              get_data_time, upload_data_pcs.run_afc_flag);

    snprintf(status_json, sizeof(status_json),
             "{\"TimeStamp\":\"%s\","
              "\"Controller_Status\":%d,"
              "\"Current_fault\":%d,"       //41021
              "\"Current_warning\":%d,"     //41022
              "\"Current_status\":%d}",     //41023
              get_data_time, upload_data_pcs.controller_status,
              modbus_read_pcs_data[12], modbus_read_pcs_data[13], modbus_read_pcs_data[14]);
/*
//delta pcs
    snprintf(data_json, sizeof(data_json),
             "{ \"TimeStamp\": \"%s\" , "
              " \"Grid_RS_Voltage\": %d , "
              " \"Grid_ST_Voltage\": %d , "
              " \"Grid_TR_Voltage\": %d , "
              " \"Total_Current_R\": %d , "
              " \"Total_Current_S\": %d , "
              " \"Total_Current_T\": %d , "
              " \"Grid_Active_Power\": %d , "
              " \"Grid_Reactive_Power\": %d , "
              " \"Power_Factor\": %d , "
              " \"Charge_Capacity\": %d , "
              " \"Discharge_Capacity\": %d , "
              " \"PCS_Status_Word\": %d , "
              " \"System_Mode\": %d , "
              " \"PCS_Control_Word\": %d }", 
              get_data_time, modbus_read_pcs_data[1-1], modbus_read_pcs_data[2-1], modbus_read_pcs_data[3-1], 
              modbus_read_pcs_data[4-1], modbus_read_pcs_data[5-1], modbus_read_pcs_data[6-1], modbus_read_pcs_data[7-1],
              modbus_read_pcs_data[20-1], modbus_read_pcs_data[21-1], modbus_read_pcs_data[29-1], modbus_read_pcs_data[32-1],
              modbus_read_pcs_data[81-1], modbus_read_pcs_data[82-1], regs[0] );
*/
/*delta pcs static
    snprintf(data_json, sizeof(data_json),
             "{ \"TimeStamp\": \"%s\" , "
              " \"Grid_RS_Voltage\": 120 , "
              " \"Grid_ST_Voltage\": 121 , "
              " \"Grid_TR_Voltage\": 122 , "
              " \"Total_Current_R\": 40 , "
              " \"Total_Current_S\": 41 , "
              " \"Total_Current_T\": 42 , "
              " \"Grid_Active_Power\": 500 , "
              " \"Grid_Reactive_Power\": 540 , "
              " \"Power_Factor\": 99 , "
              " \"Charge_Capacity\": 400 , "
              " \"Discharge_Capacity\": 500 , "
              " \"PCS_Status_Word\": 2 , "
              " \"System_Mode\": 3 , "
              " \"PCS_Control_Word\": 4 }", 
              get_data_time);
*/

    printf("pcs_data: %s\n", data_json);

    snprintf(log_cmd, sizeof(log_cmd), "echo time_data_%s_jsonlen_%d >> log/p1s/temp_log_p1s.txt", iso8601_us(time), strlen(data_json));
    system(log_cmd);

    if (!(conf_pcs = read_config(KAFKA_CONFIG_FILE)))
        printf("KAFKA_CONFIG_FILE error!");

    if (run_producer(rk_pcs, KAFKA_PCS_TOPIC, 1, data_json) == -1)
    {
        printf("run_producer_pcs error!");
        err = "run_producer_pcs error!";
    }
    if (run_producer(rk_pcs, KAFKA_AFC_TOPIC, 1, run_afc_flag_json) == -1)
    {
        printf("run_producer_afc_flag error!");
        err = "run_producer_afc_flag error!";
    }
    if (run_producer(rk_pcs, KAFKA_STATUS_TOPIC, 1, status_json) == -1)
    {
        printf("run_producer_status error!");
        err = "run_producer_status error!";
    }
    gettimeofday(&stop, NULL);

    log_insert_to_mongodb("p1s", get_data_time, (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec, err);
}

//void get_and_store_bms_data(modbus_t *modbus_client_bms, rd_kafka_t *rk, rd_kafka_conf_t *conf, char *get_data_time)
int get_and_store_bms_data(char *get_data_time, int heartbeat, config_file_data load_config_file_data)
{
	//printf("10.1----------------------------\n");
    int             ret = 0;
    uint16_t        modbus_read_input_bms_data[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        modbus_read_input_bms_status[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        modbus_read_holding_bms_data[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        Battery_string_working_status_control[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        write_data[MODBUS_MAX_READ_REGISTERS] = {0};
    //uint16_t        modbus_read_holding_bms_data[MODBUS_MAX_READ_REGISTERS] = {0};

    int             cellVoltageNumber = 576;
    int             cellTemperatureNumber = 192;
    //0x0000 : System warning, 0x0001-0x0003 : System warning, 0x0010 : System status
    //0x0020 : System voltage, 0x0021 : System Current, 0x0022 : SOC 
    int             MBMU_AddrList[] = {0x0000, 0x0001, 0x0002, 0x0003, 0x0010, 0x0020, 0x0021, 0x0022, 0x0301};
    int             SBMU_AddrList[] = {0x0000, 0x0001, 0x0002, 0x0003, 0x0020, 0x0022, 0x0023};
    int             cellVoltArrdList[cellVoltageNumber];
    int             cellTempArrdList[cellTemperatureNumber];
    uint16_t        Battery_cell_voltage_0_100[100];
    uint16_t        Battery_cell_voltage_100_200[100];
    uint16_t        Battery_cell_voltage_200_300[100];
    uint16_t        Battery_cell_voltage_300_400[100];
    uint16_t        Battery_cell_voltage_400_500[100];
    uint16_t        Battery_cell_voltage_500_576[76];
    uint16_t        Battery_cell_voltage[cellVoltageNumber];
    uint16_t        Battery_cell_temperature_0_100[100];
    uint16_t        Battery_cell_temperature_100_192[92];
    uint16_t        Battery_cell_temperature[cellTemperatureNumber];

    //bson_t          *data_db;
    //size_t          len;
    //char            *str;
    struct          timeval stop, start;
    char time[50] = {0};
    //char *get_data_time;
    char *err = "";
    char log_cmd[128] = "";

    //const char *topic;
    //const char *config_file;
    //rd_kafka_conf_t *conf;
    char data_json[100000] = "";
    char one_rack_data_json[10000] = "";
    char soc_json[100] = "";
    int soc = 0;


    snprintf(log_cmd, sizeof(log_cmd), "echo 1_time_data_%s_jsonlen_%d > log/p8s/temp_log_p8s.txt", iso8601_us(time), strlen(data_json));
    system(log_cmd);
    //gettimeofday(&start, NULL);

    for(int i = 0; i < cellVoltageNumber; i++)
    {
        cellVoltArrdList[i] = 0x0080 + i;
    }

    for(int i = 0; i < cellTemperatureNumber; i++)
    {
        cellTempArrdList[i] = 0x02C0 + i;
    }

    //get_data_time = iso8601_us(time);
    snprintf(data_json, sizeof(data_json), "{\"TimeStamp\":\"%s\",", get_data_time);

    snprintf(log_cmd, sizeof(log_cmd), "echo 2_time_data_%s_jsonlen_%d >> log/p8s/temp_log_p8s.txt", iso8601_us(time), strlen(data_json));
    system(log_cmd);

    gettimeofday(&start, NULL);

    //MBMU
    ret = modbus_read_input_registers(modbus_client_bms, MBMU_AddrList[0], 40, modbus_read_input_bms_data);
    //ret = modbus_read_input_registers(modbus_client_pcs, 1, 90, modbus_read_pcs_data);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
        err = (char *)stderr;
        modbus_client_bms = modbus_new_tcp(load_config_file_data.bms_ip, load_config_file_data.bms_port);
    } else {
        //printf("modbus_read_input_bms_data: %d\n", modbus_read_input_bms_data[1-1]);
    }
    ret = modbus_read_input_registers(modbus_client_bms, MBMU_AddrList[0], 40, modbus_read_input_bms_data);
    ret = modbus_read_input_registers(modbus_client_bms, MBMU_AddrList[8], 10, modbus_read_input_bms_status);

    soc = modbus_read_input_bms_data[34];
    snprintf(soc_json, sizeof(soc_json),
             "{\"TimeStamp\":\"%s\","
              "\"SOC\":%d}",
              get_data_time, soc);

    //write heartbeat
    write_data[0] = heartbeat;
    modbus_write_registers(modbus_client_pcs, 0x0380, 1, write_data);

    ret = modbus_read_registers(modbus_client_bms, 0x0380, 5, modbus_read_holding_bms_data);

    snprintf(one_rack_data_json, sizeof(one_rack_data_json),
              "\"%d\":%d,"
              "\"%d\":%d,"
              "\"%d\":%d,"
              "\"%d\":%d,"
              "\"%d\":%d,"
              "\"%d\":%d,"
              "\"%d\":%d,"
              "\"%d\":%d,"
              "\"%d\":%d,"
              "\"%d\":%d,"
              "\"%d\":%d,", 
              0x0380, modbus_read_holding_bms_data[0],  //EMS heartbeat
              0x0381, modbus_read_holding_bms_data[1],  //EMS_cmd
              MBMU_AddrList[0], modbus_read_input_bms_data[0], MBMU_AddrList[1], modbus_read_input_bms_data[1], 
              MBMU_AddrList[2], modbus_read_input_bms_data[2], MBMU_AddrList[3], modbus_read_input_bms_data[3], 
              MBMU_AddrList[4], modbus_read_input_bms_data[10], MBMU_AddrList[5], modbus_read_input_bms_data[20], 
              MBMU_AddrList[6], modbus_read_input_bms_data[21], MBMU_AddrList[7], modbus_read_input_bms_data[22],
              MBMU_AddrList[8], modbus_read_input_bms_status[1]);

    snprintf(data_json + strlen(data_json), sizeof(data_json) - strlen(data_json), "%s", one_rack_data_json);
    printf("bms_data: %s\n", data_json);

    //SBMU
    for(int rackIdx = 0; rackIdx < 6; rackIdx++)
    {
        //gettimeofday(&start, NULL);
        if (rackIdx > 0)
        {
            snprintf(data_json + strlen(data_json), sizeof(data_json) - strlen(data_json), ",");
            snprintf(log_cmd, sizeof(log_cmd), "echo 3_time_data_%s_jsonlen_%d >> log/p8s/temp_log_p8s.txt", iso8601_us(time), strlen(data_json));
            system(log_cmd);
            //strcat(data_json, ",");
        }

        //ret = modbus_read_input_registers(modbus_client_pcs, 1, 90, modbus_read_pcs_data);
        ret = modbus_read_input_registers(modbus_client_bms, SBMU_AddrList[0], 40, modbus_read_input_bms_data);

        //ret = modbus_read_input_registers(modbus_client_bms, rackStatAddrList[9], 2, Battery_string_working_status_control);
        //ret = modbus_read_registers(modbus_client_bms, 1, 2, modbus_read_holding_bms_data);
        /*
        for (ii=0; ii < ret; ii++) {
            printf("[%d]=%d ", ii, Battery_string_working_status_control[ii]);
        }
        */

        //printf("*****************rackStatAddrList  %x\n", rackStatAddrList[0]);

/*
        data_db = BCON_NEW("TimeStamp", BCON_UTF8(iso8601(time)),
                           rackStatAddrList[0], BCON_INT32(modbus_read_input_bms_data[1-1]),
                           rackStatAddrList[1], BCON_INT32(modbus_read_input_bms_data[2-1]),
                           rackStatAddrList[2], BCON_INT32(modbus_read_input_bms_data[3-1]),
                           rackStatAddrList[3], BCON_INT32(modbus_read_input_bms_data[4-1]),
                           rackStatAddrList[4], BCON_INT32(modbus_read_input_bms_data[63-1]),
                           rackStatAddrList[5], BCON_INT32(modbus_read_input_bms_data[64-1]),
                           rackStatAddrList[6], BCON_INT32(modbus_read_input_bms_data[65-1]),
                           rackStatAddrList[7], BCON_INT32(modbus_read_input_bms_data[66-1]),
                           rackStatAddrList[8], BCON_INT32(modbus_read_input_bms_data[67-1]),
                           rackStatAddrList[9], BCON_INT32(Battery_string_working_status_control[1-1]));
        
        str = bson_as_json(data_db, &len);
        printf ("%s\n", str);
*/
        ret = modbus_read_input_registers(modbus_client_bms, cellVoltArrdList[0], 100, Battery_cell_voltage_0_100);
        ret = modbus_read_input_registers(modbus_client_bms, cellVoltArrdList[100], 100, Battery_cell_voltage_100_200);
        ret = modbus_read_input_registers(modbus_client_bms, cellVoltArrdList[200], 100, Battery_cell_voltage_200_300);
        ret = modbus_read_input_registers(modbus_client_bms, cellVoltArrdList[300], 100, Battery_cell_voltage_300_400);
        ret = modbus_read_input_registers(modbus_client_bms, cellVoltArrdList[400], 100, Battery_cell_voltage_400_500);
        ret = modbus_read_input_registers(modbus_client_bms, cellVoltArrdList[500], 76, Battery_cell_voltage_500_576);


        for(int i = 0; i < 100; i++)
            Battery_cell_voltage[i] = Battery_cell_voltage_0_100[i];
        for(int i = 0; i < 100; i++)
            Battery_cell_voltage[100 + i] = Battery_cell_voltage_100_200[i];
        for(int i = 0; i < 100; i++)
            Battery_cell_voltage[200 + i] = Battery_cell_voltage_200_300[i];
        for(int i = 0; i < 100; i++)
            Battery_cell_voltage[300 + i] = Battery_cell_voltage_300_400[i];
        for(int i = 0; i < 100; i++)
            Battery_cell_voltage[400 + i] = Battery_cell_voltage_400_500[i];
        for(int i = 0; i < 76; i++)
            Battery_cell_voltage[500 + i] = Battery_cell_voltage_500_576[i];
        /*
        for (ii=0; ii < cellNumber; ii++) {
            //printf("[%d]=%d ", ii, Battery_cell_voltage[ii]);
        }
        */

        ret = modbus_read_input_registers(modbus_client_bms, cellVoltArrdList[0], 100, Battery_cell_temperature_0_100);
        ret = modbus_read_input_registers(modbus_client_bms, cellVoltArrdList[100], 92, Battery_cell_temperature_100_192);

        for(int i = 0; i < 100; i++)
            Battery_cell_temperature[i] = Battery_cell_temperature_0_100[i];
        for(int i = 0; i < 92; i++)
            Battery_cell_temperature[100 + i] = Battery_cell_temperature_100_192[i];
        /*
        for (ii=0; ii < cellNumber; ii++) {
            //printf("[%d]=%d ", ii, Battery_cell_temperature[ii]);
        }
        */


        //gettimeofday(&stop, NULL);
        //log_received_modbus_data("p8s", get_data_time, (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec, err);

        //gettimeofday(&start, NULL);
        //insert to kafka broker
        //topic = "bms";
        //config_file = "librdkafka.config";

        snprintf(one_rack_data_json, sizeof(one_rack_data_json),
                  "\"%d\":%d,"
                  "\"%d\":%d,"
                  "\"%d\":%d,"
                  "\"%d\":%d,"
                  "\"%d\":%d,"
                  "\"%d\":%d,"
                  "\"%d\":%d", 
                  SBMU_AddrList[0], modbus_read_input_bms_data[0], SBMU_AddrList[1], modbus_read_input_bms_data[1], 
                  SBMU_AddrList[2], modbus_read_input_bms_data[2], SBMU_AddrList[3], modbus_read_input_bms_data[3], 
                  SBMU_AddrList[4], modbus_read_input_bms_data[20], SBMU_AddrList[5], modbus_read_input_bms_data[22], 
                  SBMU_AddrList[6], modbus_read_input_bms_data[23]);

        snprintf(data_json + strlen(data_json), sizeof(data_json) - strlen(data_json), "%s", one_rack_data_json);
        //strcat(data_json, one_rack_data_json);
        snprintf(log_cmd, sizeof(log_cmd), "echo 4_time_data_%s_jsonlen_%d >> log/p8s/temp_log_p8s.txt", iso8601_us(time), strlen(data_json));
        system(log_cmd);

        for(int i = 0; i < cellVoltageNumber ;i++)
        {
            //char temp_vol[50] = "";
            //snprintf(temp_vol, sizeof(temp_vol), ",\"%d\":%d", cellVoltArrdList[i], Battery_cell_voltage[i]);

            snprintf(data_json + strlen(data_json), sizeof(data_json) - strlen(data_json), ",\"%d\":%d", cellVoltArrdList[i], Battery_cell_voltage[i]);

            //int n = snprintf(NULL, 0, "%s", data_json);
            //char data_json_temp[n + 1];
            //snprintf(data_json_temp, sizeof(data_json_temp), "%s", data_json);

            //snprintf(data_json, sizeof(data_json), "%s%s", data_json_temp, temp_vol);
            //strcat(data_json, temp_vol);
        }
        snprintf(log_cmd, sizeof(log_cmd), "echo 5_time_data_%s_jsonlen_%d >> log/p8s/temp_log_p8s.txt", iso8601_us(time), strlen(data_json));
        system(log_cmd);
        for(int i = 0; i < cellTemperatureNumber ;i++)
        {
            //char temp_tem[50] = "";
            //snprintf(temp_tem, sizeof(temp_tem), ",\"%d\":%d", cellTempArrdList[i], Battery_cell_temperature[i]);

            snprintf(data_json + strlen(data_json), sizeof(data_json) - strlen(data_json), ",\"%d\":%d", cellTempArrdList[i], Battery_cell_temperature[i]);

            //memccpy(data_json, temp_tem, '\0', sizeof(temp_tem));
            //strcat(data_json, temp_tem);
        }
        snprintf(log_cmd, sizeof(log_cmd), "echo 6_time_data_%s_jsonlen_%d >> log/p8s/temp_log_p8s.txt", iso8601_us(time), strlen(data_json));
        system(log_cmd);
        

        printf("Rack :%d  ", rackIdx);

        for(int i = 0; i < 6 ;i++)
        {
            SBMU_AddrList[i] += 0x0400;
        }
        for(int i = 0; i < cellVoltageNumber; i++)
        {
            cellVoltArrdList[i] += 0x0400;
        }
        for(int i = 0; i < cellTemperatureNumber; i++)
        {
            cellTempArrdList[i] += 0x0400;
        }
    }
    printf("\n");
    snprintf(data_json + strlen(data_json), sizeof(data_json) - strlen(data_json), "}");
    //strcat(data_json, "}");

    //printf("data_json :%s\n", data_json);

    snprintf(log_cmd, sizeof(log_cmd), "echo 7_time_data_%s_jsonlen_%d >> log/p8s/temp_log_p8s.txt", iso8601_us(time), strlen(data_json));
    system(log_cmd);

    gettimeofday(&stop, NULL);
    log_received_modbus_data("p8s", get_data_time, (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec, err);

    gettimeofday(&start, NULL);

    if (!(conf_bms = read_config(KAFKA_CONFIG_FILE)))
        printf("KAFKA_CONFIG_FILE error!");

    if (run_producer(rk_bms, KAFKA_BMS_TOPIC, 1, data_json) == -1)
    {
        printf("run_producer_bms error!");
        err = "run_producer_bms error!";
    }
    if (run_producer(rk_bms, KAFKA_SOC_TOPIC, 1, soc_json) == -1)
    {
        printf("run_producer_soc error!");
        err = "run_producer_soc error!";
    }

    gettimeofday(&stop, NULL);

    log_insert_to_mongodb("p8s", get_data_time, (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec, err);

    return soc;
}
/*
void *foo_1s(void *argu)
{
    struct thread_para thread_para_pcs = * (struct thread_para*) argu;
    modbus_t *modbus_client_pcs = thread_para_pcs.modbus_client;
    rd_kafka_t *rk = thread_para_pcs.rk;
    rd_kafka_conf_t *conf = thread_para_pcs.conf;
    struct timespec req;
    int ret = 0;
    unsigned int real_sleep_time = 0;
    int sleep_delay = 0;

    //req.tv_sec = 1;
    //req.tv_nsec = 1000000;


    while (1) {    
        char now_time[50] = {0};
        char *get_data_time;
        struct timeval stop, start, sleep_start, sleep_stop;
        int execution_time = 0;
        int sleep_time = 0;


        gettimeofday(&start, NULL);
        get_data_time = iso8601_us(now_time);
        get_and_store_pcs_data(modbus_client_pcs, rk, conf, get_data_time);
        gettimeofday(&stop, NULL);


        execution_time = (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec;
        sleep_time = 1000000 - execution_time;
        //log_total_running_time("p1s", get_data_time, execution_time, sleep_time);

        printf("1s took %u us\n", execution_time);
        printf("1s  %s\n", get_data_time);
        printf("1s  sleep time : %d us\n", sleep_time);
        if(sleep_time > 0 )
        {
            gettimeofday(&sleep_start, NULL);
            req.tv_nsec = (1000000 - execution_time - sleep_delay) * 1000;
            printf("1s  req.tv_nsec : %d us\n", req.tv_nsec);
            //usleep(1000000 - execution_time - sleep_delay);
            
            ret = nanosleep(&req, NULL);
            if (-1 == ret)
            {
            	fprintf (stderr, "\t nanousleep   not support\n");
            }
            
            gettimeofday(&sleep_stop, NULL);
            real_sleep_time = (sleep_stop.tv_sec - sleep_start.tv_sec) * 1000000 + sleep_stop.tv_usec - sleep_start.tv_usec;
            if(real_sleep_time - sleep_time > 0 )
            {
            	sleep_delay = real_sleep_time - sleep_time;
        	}
            printf("1s  real_sleep_time : %d us\n", real_sleep_time);
            printf("1s  sleep_delay : %d us\n", sleep_delay);
        }
        //usleep(200000);
    }    
    return NULL; 
}
*/
/*
void *foo_1s(void *argu)
{
    struct thread_para thread_para_pcs = * (struct thread_para*) argu;
    modbus_t *modbus_client_pcs = thread_para_pcs.modbus_client;
    rd_kafka_t *rk = thread_para_pcs.rk;
    rd_kafka_conf_t *conf = thread_para_pcs.conf;

    while (1) {    
        char now_time[50] = {0};
        char *get_data_time;
        struct timeval stop, start;
        int execution_time = 0;
        int sleep_time = 0;


        gettimeofday(&start, NULL);
        get_data_time = iso8601_us(now_time);
        get_and_store_pcs_data(modbus_client_pcs, rk, conf, get_data_time);
        gettimeofday(&stop, NULL);


        execution_time = (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec;
        log_total_running_time("p1s", get_data_time, execution_time, sleep_time);

        sleep_time = 1000000 - execution_time;
        printf("1s took %u us\n", execution_time);
        printf("1s  %s\n", get_data_time);
        printf("1s  sleep time : %d us\n", sleep_time);
        
        if(sleep_time > 0 )
        {
        	usleep(sleep_time);
        }
        
        //usleep(200000);
    }    
    return NULL; 
}
*/

upload_data check_controller_modbus_server(int soc, double Freq_now, config_file_data load_config_file_data)
{
    int             ret;
    uint16_t        modbus_read_controller_data[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        modbus_read_pcs_data[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        modbus_read_pcs_status[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        modbus_read_input_bms_data[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        regs[MODBUS_MAX_READ_REGISTERS] = {0};
    uint16_t        write_data[MODBUS_MAX_READ_REGISTERS] = {0};
    char *err = "";

    struct  upload_data upload_data_pcs;
    double freq_base = 60.0;
    double compensate = 0;
    double power_factor = 0;
    //double power_output = 0;
    int run_afc_flag = 0;
    char setting_cmd[128] = "";

    upload_data_pcs.execution_capacity = 2640;  //kw
    upload_data_pcs.pcs_nominal_power = 2640;   //kw
    upload_data_pcs.run_afc_flag = 0;
    upload_data_pcs.controller_status = 0;
    upload_data_pcs.freq_base = 0;
    upload_data_pcs.compensate = 0;
    upload_data_pcs.power_output = 0;
    upload_data_pcs.power_original = 0;

    printf("default_power_compensate_server1:%d\n", load_config_file_data.default_power_compensate);
    compensate = load_config_file_data.default_power_compensate * 0.01;
    printf("default_power_compensate_server2:%d\n", load_config_file_data.default_power_compensate);
    printf("compensate_server1:%f\n", compensate);

    //controller modbus server check
    ret = modbus_read_registers(modbus_client_controller, 0, 1, modbus_read_controller_data);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
        err = (char *)stderr;
        modbus_client_controller = modbus_new_tcp(load_config_file_data.controller_modbus_server_ip, load_config_file_data.controller_modbus_server_port);
        printf("reconnect modbus controller\n");
    } else {
        //printf("modbus controller: %d\n", modbus_read_controller_data[0]);
    }

    //pcs modbus check
    ret = modbus_read_registers(modbus_client_pcs, 41011, 1, modbus_read_pcs_data);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
        err = (char *)stderr;
        modbus_client_pcs = modbus_new_tcp(load_config_file_data.pcs_ip, load_config_file_data.pcs_port);
        printf("reconnect modbus controller\n");
    } else {
        //printf("modbus controller pcs: %d\n", modbus_read_pcs_data[0]);
    }

    //bms modbus check
    ret = modbus_read_input_registers(modbus_client_bms, 0x0300, 10, modbus_read_input_bms_data);
    //ret = modbus_read_input_registers(modbus_client_pcs, 1, 90, modbus_read_pcs_data);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
        err = (char *)stderr;
        modbus_client_bms = modbus_new_tcp(load_config_file_data.bms_ip, load_config_file_data.bms_port);
    } else {
        //printf("modbus_read_input_bms_data: %d\n", modbus_read_input_bms_data[1-1]);
    }

    //ret = modbus_read_input_registers(modbus_client_controller, 1, 1, modbus_read_controller_data);
    //read controller modbus server
    ret = modbus_read_registers(modbus_client_controller, 0, 5, modbus_read_controller_data);
    //read pcs power factor
    ret = modbus_read_registers(modbus_client_pcs, 41011, 1, modbus_read_pcs_data);
    //read pcs status
    ret = modbus_read_registers(modbus_client_pcs, 41123, 1, modbus_read_pcs_status);

    ret = modbus_read_registers(modbus_client_bms, 0x0300, 10, modbus_read_input_bms_data);

    //freq_base = modbus_read_controller_data[1];
    //compensate = modbus_read_controller_data[2];
    //freq_base = 60.0;
    //compensate = 1.05;
    //Freq_now = 59.50;

    //start
    if (modbus_read_controller_data[0] == 1) {
        //start bms
        if(modbus_read_input_bms_data[1] != 1)
        {
            write_data[0] = 2;
            modbus_write_registers(modbus_client_pcs, 0x0381, 1, write_data);

            //pcs status = on
            if(modbus_read_pcs_status[0] != 4)
            {
                //start pcs
                write_data[0] = 1;
                modbus_write_registers(modbus_client_pcs, 43010, 1, write_data);
            }
        }

    //stop
    } else if (modbus_read_controller_data[0] == 2) {
      	//stop pcs
        if(modbus_read_pcs_status[0] != 2)
        {
            write_data[0] = 1;
            modbus_write_registers(modbus_client_pcs, 43200, 1, write_data);

            //stop bms
            if(modbus_read_input_bms_data[1] != 0)
            {
                write_data[0] = 3;
                modbus_write_registers(modbus_client_pcs, 0x0381, 1, write_data);
            }
        }

    //run
    } else if (modbus_read_controller_data[0] == 3) {
        //pcs status = on
        //if(modbus_read_pcs_status[0] == 4)
        if(1)
        {
            //start pq mode
          	write_data[0] = 2;
          	modbus_write_registers(modbus_client_pcs, 40551, 1, write_data);

            //set output power
            if(modbus_read_controller_data[1] != 0)
                upload_data_pcs.execution_capacity = modbus_read_controller_data[1];
            if(modbus_read_controller_data[2] != compensate)
            {
                snprintf(setting_cmd, sizeof(setting_cmd), "echo %d > power_compensate", modbus_read_controller_data[2]);
                system(setting_cmd);
                compensate = modbus_read_controller_data[2] * 0.01;
            }
            if(modbus_read_controller_data[3] != 0)
                freq_base = modbus_read_controller_data[3] * 0.01;
            power_factor = modbus_read_pcs_data[0] * 0.001;
            //freq_base = 60;
            upload_data_pcs = power_compensate(freq_base, Freq_now, compensate, soc, power_factor, upload_data_pcs);
            write_data[0] = upload_data_pcs.power_output;
            modbus_write_registers(modbus_client_pcs, 40553, 1, write_data);

            run_afc_flag = 1;
        } else {
            write_data[0] = 0;
            modbus_write_registers(modbus_client_controller, 0, 1, write_data);
        }

    //standby
    } else if (modbus_read_controller_data[0] == 4) {
      	write_data[0] = 1;
      	modbus_write_registers(modbus_client_pcs, 43011, 1, write_data);

    //default
    } else {
        write_data[0] = 0;
        modbus_write_registers(modbus_client_controller, 0, 1, write_data);
    }

    //power_output = power_compensate(freq_base, Freq_now, compensate, soc);

    upload_data_pcs.run_afc_flag = run_afc_flag;
    upload_data_pcs.controller_status = modbus_read_controller_data[0];
    upload_data_pcs.freq_base = freq_base;
    upload_data_pcs.compensate = compensate;
    //upload_data_pcs.power_output = power_output;

    printf("load_config_file_data.default_power_compensate: %f\n", load_config_file_data.default_power_compensate); 
    printf("compensate: %f\n", compensate);
    printf("upload_data_pcs.compensate: %f\n", upload_data_pcs.compensate);
    printf("modbus_read_controller_status: %d\n", modbus_read_controller_data[0]);

    return upload_data_pcs;
}

void *foo_1s(void *argu)
{
    struct  config_file_data load_config_file_data = * (struct config_file_data*) argu;
    //struct thread_para thread_para_pcs = * (struct thread_para*) argu;
    //modbus_t *modbus_client_pcs = thread_para_pcs.modbus_client;
    //modbus_t *modbus_client_controller = thread_para_pcs.modbus_client_controller;
    //rd_kafka_t *rk = thread_para_pcs.rk;
    //rd_kafka_conf_t *conf = thread_para_pcs.conf;
    struct timeval time_flag, time_s_flag;
    int heartbeat = 1;

    while (1) {
    	gettimeofday(&time_flag, NULL);
    	//printf("1s time_flag.tv_usec %u us\n", time_flag.tv_usec);
    	//if((time_s_flag.tv_sec != time_flag.tv_sec) && (time_flag.tv_usec > 500000))
      if(time_s_flag.tv_sec != time_flag.tv_sec)
    	{
          printf("\nstart-------------------------------------------------------------\n");
    		  //printf("1s time_flag.tv_usec %u us\n", time_flag.tv_usec);
	        char now_time[50] = {0};
	        char *get_data_time;
	        struct timeval stop, start;
	        int execution_time = 0;
	        int sleep_time = 0;
          double Freq_now = 0;
          struct  upload_data upload_data_pcs;

          int soc = 0;
          //int run_afc_flag = 0;

          if(heartbeat > 255)
              heartbeat = 1;

	        gettimeofday(&start, NULL);
	        get_data_time = iso8601_us(now_time);
	        //get_and_store_pcs_data(modbus_client_pcs, rk, conf, get_data_time);
	        //check_controller_modbus_server(modbus_client_controller, modbus_client_pcs);
          //printf("load_config_file_data.default_power_compensate_foo1s: %f\n", load_config_file_data.default_power_compensate);
          printf("pcs_ip_foo1s:%s:%d\n", load_config_file_data.pcs_ip, load_config_file_data.pcs_port);
          printf("bms_ip_foo1s:%s:%d\n", load_config_file_data.bms_ip, load_config_file_data.bms_port);
          printf("controller_modbus_server_ip_foo1s:%s:%d\n", load_config_file_data.controller_modbus_server_ip, load_config_file_data.controller_modbus_server_port);
          printf("meter_ip_foo1s:%s:%d\n", load_config_file_data.meter_ip, load_config_file_data.meter_port);
          printf("default_power_compensate_foo1s_1:%d\n", load_config_file_data.default_power_compensate);
          Freq_now = iec61850_get(get_data_time, load_config_file_data);
          printf("default_power_compensate_foo1s_2:%d\n", load_config_file_data.default_power_compensate);
          soc = get_and_store_bms_data(get_data_time, heartbeat, load_config_file_data);
          printf("default_power_compensate_foo1s_3:%d\n", load_config_file_data.default_power_compensate);
	        upload_data_pcs = check_controller_modbus_server(soc, Freq_now, load_config_file_data);
          printf("default_power_compensate_foo1s_4:%d\n", load_config_file_data.default_power_compensate);
          get_and_store_pcs_data(get_data_time, upload_data_pcs, heartbeat, load_config_file_data);
	        gettimeofday(&stop, NULL);


	        execution_time = (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec;
	        sleep_time = 1000000 - execution_time;
	        log_total_running_time("p1s", get_data_time, execution_time, sleep_time);

	        time_s_flag.tv_sec = time_flag.tv_sec;
	        
	        printf("1s took %u us\n", execution_time);
	        printf("1s  %s\n", get_data_time);

          heartbeat ++;
	        //printf("1s  sleep time : %d us\n", sleep_time);
	        /*
	        if(sleep_time > 0 )
	        {
	        	usleep(sleep_time);
	        }
	        */
	        //usleep(200000);
	    }
    }    
    return NULL; 
}

void *foo_8s(void *argu)
{
    //struct thread_para thread_para_bms = * (struct thread_para*) argu;
    //modbus_t *modbus_client_bms = thread_para_bms.modbus_client;
    //rd_kafka_t *rk = thread_para_bms.rk;
    //rd_kafka_conf_t *conf = thread_para_bms.conf;
    struct timeval time_flag, time_s_flag;

    gettimeofday(&time_flag, NULL);
    time_s_flag.tv_sec = time_flag.tv_sec;

    while (1) {
    	//printf("time_flag----------------------------%d\n", time_s_flag.tv_sec + 8);
    	gettimeofday(&time_flag, NULL);
    	if(((time_s_flag.tv_sec + 8) == time_flag.tv_sec) && (time_flag.tv_usec > 500000))
    	//if((time_s_flag.tv_sec != time_flag.tv_sec) && (time_flag.tv_usec > 500000))
    	{
      		//printf("8----------------------------%d\n", time_s_flag.tv_sec + 8);
  	    	gettimeofday(&time_flag, NULL);
	        char now_time[50] = {0};
	        char *get_data_time;
	        struct timeval stop, start;
	        int execution_time = 0;
	        int sleep_time = 0;


	        gettimeofday(&start, NULL);
	        get_data_time = iso8601_us(now_time);
	        //get_and_store_bms_data(modbus_client_bms, rk, conf, get_data_time);
	        //get_and_store_bms_data(get_data_time);
	        gettimeofday(&stop, NULL);


	        execution_time = (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec;
	        sleep_time = 8000000 - execution_time;
	        log_total_running_time("p8s", get_data_time, execution_time, sleep_time);

	        time_s_flag.tv_sec = time_flag.tv_sec;
	        
	        printf("8s took %u us\n", execution_time);
	        printf("8s  %s", get_data_time);
	        printf("8s  sleep time : %d us\n", sleep_time);
	        //usleep(sleep_time);    
	        //usleep(200000);
	    }
    }    
    return NULL; 
}

config_file_data load_config_file()
{
    config_file_data load_config_file_data;
    FILE *fp_config;
    FILE *fp_compensate;
    //char *line = NULL;
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    //char buf[100] = {0};
    const char * const delimiter = "=:";

    //line = malloc(sizeof(char) * (100 + 1));

    fp_config = fopen("afc_controller.config", "r");
    if (fp_config == NULL)
    {
        printf("File Not Found");
        //return 0;
    }
    else
    {
        char *line = NULL;
        size_t len = 0;
        ssize_t read;
        while ((read = getline(&line, &len, fp_config) != -1))
        {
            int index = 0;
            char *substr = NULL;
            char config_topic[100] = "";
            char *config_topic_pcs_ip = "pcs_ip";
            printf("Retrieved line of length %zu \n", read);
            printf("line: %s", line);

            //substr = malloc(sizeof(char) * (100 + 1));
            substr = strtok(line, delimiter);
            do {
                if(index == 0)
                {
                    //config_topic = substr;
                    snprintf(config_topic, sizeof(config_topic), substr);
                    //printf("1config_topic: %s---------------\n", config_topic);
                    //printf("substr: %s---------------\n", substr);
                }
                else if(index == 1)
                {
                    //printf("2config_topic: %s------------------------\n", config_topic);
                    if(strncmp(config_topic, "pcs_ip", 6) == 0)
                    {
                        //load_config_file_data.pcs_ip = substr;
                        snprintf(load_config_file_data.pcs_ip, sizeof(load_config_file_data.pcs_ip), substr);
                        //printf("------------------------pcs_ip:%s\n", load_config_file_data.pcs_ip);
                    }
                    else if(strncmp(config_topic, "bms_ip", 6) == 0)
                        snprintf(load_config_file_data.bms_ip, sizeof(load_config_file_data.bms_ip), substr);
                    else if(strncmp(config_topic, "controller_modbus_server_ip", 27) == 0)
                        snprintf(load_config_file_data.controller_modbus_server_ip, sizeof(load_config_file_data.controller_modbus_server_ip), substr);
                    else if(strncmp(config_topic, "meter_ip", 8) == 0)
                        snprintf(load_config_file_data.meter_ip, sizeof(load_config_file_data.meter_ip), substr);
                }
                else if(index == 2)
                {
                    if(strncmp(config_topic, "pcs_ip", 6) == 0)
                        load_config_file_data.pcs_port = atoi(substr);
                    else if(strncmp(config_topic, "bms_ip", 6) == 0)
                        load_config_file_data.bms_port = atoi(substr);
                    else if(strncmp(config_topic, "controller_modbus_server_ip", 27) == 0)
                        load_config_file_data.controller_modbus_server_port = atoi(substr);
                    else if(strncmp(config_topic, "meter_ip", 8) == 0)
                        load_config_file_data.meter_port = atoi(substr);
                }
                printf("#%d sub string: %s\n", index, substr);
                substr = strtok(NULL, delimiter);
                index++;
                //printf("555------------------------pcs_ip:%s\n", load_config_file_data.pcs_ip);
            } while (substr);
            //printf("666------------------------pcs_ip:%s\n", load_config_file_data.pcs_ip);
            if (substr)
            {
                free(substr);
            }
        }
        fclose(fp_config);
        if (line)
        {
            free(line);
        }
        //printf("2------------------------pcs_ip:%s\n", load_config_file_data.pcs_ip);
    }


    fp_compensate = fopen("power_compensate", "r");
    if (fp_compensate == NULL)
    {
        printf("File Not Found");
    }
    else
    {
        char *line = NULL;
        size_t len = 0;
        ssize_t read;
        while ((read = getline(&line, &len, fp_compensate) != -1))
        {
            printf("Retrieved line of length %zu \n", read);
            printf("line: %s", line);
            load_config_file_data.default_power_compensate = atoi(line);
        }
        fclose(fp_compensate);
        if (line)
        {
            free(line);
        }
        //printf("2------------------------pcs_ip:%s\n", load_config_file_data.pcs_ip);
    }
    return load_config_file_data;
}

void test_freq_pq()
{
    //dReg 0.25
    double freq_base = 60.00;
    int soc = 760;
    double freq_test[] = {60.01, 59.99, 60.02, 59.98, 60.07, 59.93, 60.14, 59.86, 60.20, 59.80, 60.25, 59.75, 60.30, 59.70, 60.40, 59.60, 60.50, 59.50};
    int sizeof_freq_test = sizeof(freq_test) / sizeof(freq_test[0]);
    double power_output = 0;

    printf("freq_test : %f \n\n", freq_test[0]);

    printf("sizeof_freq_test : %d \n", sizeof_freq_test);

    for(int i = 0 ; i < sizeof_freq_test; i++)
    {
        power_output = freq_pq(freq_base, freq_test[i], soc);
        printf("#%d power_output : %f \n\n", i+1, power_output);
    }
}

int main(int argc, char *argv[])
{
    struct  config_file_data load_config_file_data;
    //char now_time[50] = {0};
    //printf("--------------------  %s", iso8601_us(now_time));

    //kafka
    //rd_kafka_t *rk_pcs;
    char errstr_pcs[512];
    struct  thread_para thread_para_pcs;
    //rd_kafka_conf_t *conf_pcs;

    //rd_kafka_t *rk_bms;
    char errstr_bms[512];
    struct  thread_para thread_para_bms;
    //rd_kafka_conf_t *conf_bms;

    char errstr_meter[512];

    const char *config_file = KAFKA_CONFIG_FILE;
    int msgcnt = 1;
    int delivery_counter = 0;


    //modbus
    //modbus_t        *modbus_client_pcs;
    //modbus_t        *modbus_client_bms;
    //modbus_t        *modbus_client_controller;
    struct timeval  timeout;
    uint32_t        timeout_sec;
    uint32_t        timeout_usec;
    int             ret, ii;
    //uint8_t         bits[MODBUS_MAX_READ_BITS] = {0};
    uint16_t        regs[MODBUS_MAX_READ_REGISTERS] = {0};
 


    load_config_file_data = load_config_file();
    printf("pcs_ip:%s:%d\n", load_config_file_data.pcs_ip, load_config_file_data.pcs_port);
    printf("bms_ip:%s:%d\n", load_config_file_data.bms_ip, load_config_file_data.bms_port);
    printf("controller_modbus_server_ip:%s:%d\n", load_config_file_data.controller_modbus_server_ip, load_config_file_data.controller_modbus_server_port);
    printf("meter_ip:%s:%d\n", load_config_file_data.meter_ip, load_config_file_data.meter_port);
    printf("default_power_compensate:%d\n", load_config_file_data.default_power_compensate);

    test_freq_pq();

    //printf("1----------------------------\n");
    //pcs
    modbus_client_pcs = modbus_new_tcp(load_config_file_data.pcs_ip, load_config_file_data.pcs_port);
 
    // set device ID 
    modbus_set_slave(modbus_client_pcs, MODBUS_DEVICE_ID);
 
    // Debug mode 
    modbus_set_debug(modbus_client_pcs, MODBUS_DEBUG);
 
    // set timeout 
    timeout_sec = MODBUS_TIMEOUT_SEC;
    timeout_usec = MODBUS_TIMEOUT_USEC;
    modbus_get_byte_timeout(modbus_client_pcs, &timeout_sec, &timeout_usec);
 
    timeout_sec = MODBUS_TIMEOUT_SEC;
    timeout_usec = MODBUS_TIMEOUT_USEC;
    modbus_set_response_timeout(modbus_client_pcs, timeout_sec, timeout_usec);
 
    if (modbus_connect(modbus_client_pcs) == -1) {
        fprintf(stderr, "modbus_client_pcs Connexion failed: %s\n",
                modbus_strerror(errno));
        modbus_free(modbus_client_pcs);
        exit(-1);
    }



    //bms
    modbus_client_bms = modbus_new_tcp(load_config_file_data.bms_ip, load_config_file_data.bms_port);
    /* set device ID */
    modbus_set_slave(modbus_client_bms, MODBUS_DEVICE_ID);
 
    /* Debug mode */
    modbus_set_debug(modbus_client_bms, MODBUS_DEBUG);
 
    /* set timeout */
    timeout_sec = MODBUS_TIMEOUT_SEC;
    timeout_usec = MODBUS_TIMEOUT_USEC;
    modbus_get_byte_timeout(modbus_client_bms, &timeout_sec, &timeout_usec);
 
    timeout_sec = MODBUS_TIMEOUT_SEC;
    timeout_usec = MODBUS_TIMEOUT_USEC;
    modbus_set_response_timeout(modbus_client_bms, timeout_sec, timeout_usec);


    if (modbus_connect(modbus_client_bms) == -1) {
        fprintf(stderr, "modbus_client_bms Connexion failed: %s\n", modbus_strerror(errno));
        modbus_free(modbus_client_bms);
        exit(-1);
    }

    //controller
    modbus_client_controller = modbus_new_tcp(load_config_file_data.controller_modbus_server_ip, load_config_file_data.controller_modbus_server_port);
    /* set device ID */
    modbus_set_slave(modbus_client_controller, MODBUS_DEVICE_ID);
 
    /* Debug mode */
    modbus_set_debug(modbus_client_controller, MODBUS_DEBUG);
 
    /* set timeout */
    timeout_sec = MODBUS_TIMEOUT_SEC;
    timeout_usec = MODBUS_TIMEOUT_USEC;
    modbus_get_byte_timeout(modbus_client_controller, &timeout_sec, &timeout_usec);
 
    timeout_sec = MODBUS_TIMEOUT_SEC;
    timeout_usec = MODBUS_TIMEOUT_USEC;
    modbus_set_response_timeout(modbus_client_controller, timeout_sec, timeout_usec);


    if (modbus_connect(modbus_client_controller) == -1) {
        fprintf(stderr, "modbus_client_controller Connexion failed: %s\n", modbus_strerror(errno));
        modbus_free(modbus_client_controller);
        exit(-1);
    }
    //printf("2----------------------------\n");
/*
    // read input registers (0x04 function code) 
    ret = modbus_read_input_registers(modbus_client_pcs, MODUBS_INPUT_ADDR, MODUBS_INPUT_LEN, regs);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    } else {
        printf("INPUT REGISTERS:\n");
        for (ii=0; ii < ret; ii++) {
            printf("[%d]=%d ", ii, regs[ii]);
        }
    }
 */
    /*
    // read holding registers (0x03 function code) 
    ret = modbus_read_registers(ctx, MODUBS_HOLDING_ADDR, MODUBS_HOLDING_LEN, regs);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    } else {
        printf("HOLDING REGISTERS:\n");
        for (ii=0; ii < ret; ii++) {
            printf("[%d]=%d\n", ii, regs[ii]);
        }
    }
    */
 /*
 
    // write single register (0x06 function code) 
    ret = modbus_write_register(ctx, 1, 0x1234);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    }
 
    // write multi registers (0x10 function code) 
    regs[1] = 0x1234;
    regs[2] = 0x5678;
    ret = modbus_write_registers(ctx, 1, 2, &regs[1]);
    if (ret < 0) {
        fprintf(stderr, "%s\n", modbus_strerror(errno));
    }
 */

    if (!(conf_pcs = read_config(config_file)))
        return 1;
    if (!(conf_bms = read_config(config_file)))
        return 1;
    if (!(conf_meter = read_config(config_file)))
        return 1;

    //kafka pcs
    rd_kafka_conf_set_dr_msg_cb(conf_pcs, dr_cb);

    /* Create producer.
     * A successful call assumes ownership of \p conf. */
    rk_pcs = rd_kafka_new(RD_KAFKA_PRODUCER, conf_pcs, errstr_pcs, sizeof(errstr_pcs));
    if (!rk_pcs) {
            fprintf(stderr, "Failed to create producer: %s\n", errstr_pcs);
            rd_kafka_conf_destroy(conf_pcs);
            return -1;
    }

    /* Create the topic. */
    if (create_topic(rk_pcs, KAFKA_PCS_TOPIC, 1) == -1) {
            rd_kafka_destroy(rk_pcs);
            return -1;
    }


    //kafka bms
    rd_kafka_conf_set_dr_msg_cb(conf_bms, dr_cb);

    /* Create producer.
     * A successful call assumes ownership of \p conf. */
    rk_bms = rd_kafka_new(RD_KAFKA_PRODUCER, conf_bms, errstr_bms, sizeof(errstr_bms));
    if (!rk_bms) {
            fprintf(stderr, "Failed to create producer: %s\n", errstr_bms);
            rd_kafka_conf_destroy(conf_bms);
            return -1;
    }

    /* Create the topic. */
    if (create_topic(rk_bms, KAFKA_BMS_TOPIC, 1) == -1) {
            rd_kafka_destroy(rk_bms);
            return -1;
    }

    //kafka meter
    rd_kafka_conf_set_dr_msg_cb(conf_meter, dr_cb);

    /* Create producer.
     * A successful call assumes ownership of \p conf. */
    rk_meter = rd_kafka_new(RD_KAFKA_PRODUCER, conf_meter, errstr_meter, sizeof(errstr_meter));
    if (!rk_meter) {
            fprintf(stderr, "Failed to create producer: %s\n", errstr_meter);
            rd_kafka_conf_destroy(conf_meter);
            return -1;
    }

    /* Create the topic. */
    if (create_topic(rk_meter, KAFKA_METER_TOPIC, 1) == -1) {
            rd_kafka_destroy(rk_meter);
            return -1;
    }

    //IEC61850
    con_meter = IedConnection_create();
    IedConnection_connect(con_meter, &error_meter, load_config_file_data.meter_ip, load_config_file_data.meter_port);
    //IedConnection_connect(con_meter, &error_meter, IEC61850_SERVER_HOST, IEC61850_SERVER_PORT);
    //iec61850_get();

    /*
    struct timeval stop, start;
    gettimeofday(&start, NULL);
    //do stuff
    gettimeofday(&stop, NULL);
    printf("took %lu us\n", (stop.tv_sec - start.tv_sec) * 1000000 + stop.tv_usec - start.tv_usec);
    */

    //log_received_modbus_data("p1s");

    pthread_t thread1s, thread8s;
    
    thread_para_pcs.modbus_client = modbus_client_pcs;
    thread_para_pcs.modbus_client_controller = modbus_client_controller;
    thread_para_pcs.rk = rk_pcs;
    thread_para_pcs.conf = conf_pcs;
    //pthread_create(&thread1s, NULL, &foo_1s, (void*) &thread_para_pcs);
    pthread_create(&thread1s, NULL, &foo_1s, (void*) &load_config_file_data);

    /*
    thread_para_bms.modbus_client = modbus_client_bms;
    thread_para_bms.modbus_client_controller = modbus_client_controller;
    thread_para_bms.rk = rk_bms;
    thread_para_bms.conf = conf_bms;
    */
    //pthread_create(&thread8s, NULL, &foo_8s, (void*) &thread_para_bms);
    //pthread_create(&thread8s, NULL, &foo_8s, (void*) &thread_para_bms);
    
    while(1) {
        //printf("----------------\n");
        //usleep(10000);
    }
    


    /* Close the connection */
    modbus_close(modbus_client_pcs);
    modbus_free(modbus_client_pcs);
    modbus_close(modbus_client_bms);
    modbus_free(modbus_client_bms);
    modbus_close(modbus_client_controller);
    modbus_free(modbus_client_controller);

    /* Destroy the producer instance. */
    rd_kafka_destroy(rk_pcs);
    rd_kafka_destroy(rk_bms);
    rd_kafka_destroy(rk_meter);

    IedConnection_close(con_meter);
    IedConnection_destroy(con_meter);

    fprintf(stderr, "%d/%d messages delivered\n",
            delivery_counter, msgcnt);

    return 0;
}

/*
int main(int argc, char *argv[])
{
    const char *topic;
    const char *config_file;
    rd_kafka_conf_t *conf;

    
//    if (argc != 3) {
//            fprintf(stderr, "Usage: %s <topic> <config-file>\n", argv[0]);
//            exit(1);
//    }
    

    //topic = argv[1];
    //config_file = argv[2];

    topic = "test1";
    config_file = "librdkafka.config";

    if (!(conf = read_config(config_file)))
        return 1;

    if (run_producer(topic, 1, conf) == -1)
        return 1;

    return 0;
}
*/