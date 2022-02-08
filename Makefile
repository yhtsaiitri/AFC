#gcc get_store.c common.c json.c -o get_store.out -lrdkafka -lm -lpthread -std=gnu99 $(pkg-config --libs --cflags /root/modbus/libmodbus-3.1.6/libmodbus.pc)
#LIBIEC_HOME=.
LIBIEC_HOME=libiec61850/

PROJECT_BINARY_NAME = get_store.out
PROJECT_SOURCES += get_store.c 
PROJECT_SOURCES += common.c 
PROJECT_SOURCES += json.c 

CFLAGS += -std=gnu99 
CFLAGS += -lrdkafka 
CFLAGS += -lm 
CFLAGS += -lpthread 
#CFLAGS += /root/modbus/libmodbus-3.1.6/libmodbus.pc
CC = gcc
LDFLAGS = `pkg-config --libs --cflags /root/modbus/libmodbus-3.1.6/libmodbus.pc`

include $(LIBIEC_HOME)/make/target_system.mk
include $(LIBIEC_HOME)/make/stack_includes.mk

all:	$(PROJECT_BINARY_NAME)

include $(LIBIEC_HOME)/make/common_targets.mk


$(PROJECT_BINARY_NAME):	$(PROJECT_SOURCES) $(LIB_NAME)
	$(CC) $(PROJECT_SOURCES) -o $(PROJECT_BINARY_NAME) $(INCLUDES) $(CFLAGS) $(INCLUDES) $(LIB_NAME) $(LDLIBS) $(LDFLAGS) 

clean:
	rm -f $(PROJECT_BINARY_NAME)