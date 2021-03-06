#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <libaio.h>
#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <fcntl.h>
#include <features.h>
#include <sys/vfs.h>
#include <arpa/inet.h>
#include <assert.h>

//#define DEBUG_OEPN
//#undef DEBUG_OPEN

//#ifdef DEBUG_OPEN
//	#define pr_debug(fmt, args...) printf("[%s() L.%d]: "fmt, __func__, __LINE__, ## args)
//#else
	#define pr_debug(fmt, args...)
//#endif

typedef unsigned long long ull;

//************* Data Structure Definition ****************

struct Config{
    int Device; //fd for block device 
    FILE *TraceFile;        
    FILE *ResultFile;  //FILE for trace file & result file
    int Test_Time; //Max traceplay time (second) 
    int sync; //sync or async flag
    int sleep_us;

    int Buffer_Needed;
    int Firsthalf;
   

    int played;
    int persisted;

    struct Trace_Entry *Trace_Buffer;   //buffer for scanned trace entries 
    struct Record *Record_Buffer;   //buffer for TracePlay results

    struct Queue_Entry *IOQueue;    //Link_list Buffer for IOs in flight
    struct Queue_Entry *head;
    struct Queue_Entry *tail;
    int Queue_Free; //how many available buffers 

    ull First_Request_us;
    ull Trace_Start_us;
    ull Nr_Trace_Read;
    int Nr_Flight_IOs;  //# of IOs in flight
    int First_Entry_Flag;
    int should_stop;

    io_context_t ctx;

    pthread_t Reap_th; //thread for handling return IOs
    pthread_t TBufferManage_th;  //thread for managing the Buffer of tracefile
    pthread_t RBufferManage_th;  //thread for managing the Buffer of resultfile

    pthread_spinlock_t spinlock;    //spinlock for Queue_Entry allocation and recycling
    pthread_mutex_t mutex;  
    pthread_cond_t cond;
    pthread_mutex_t mutex2;  
    pthread_cond_t cond2;
    pthread_mutex_t mutex3;  
    pthread_cond_t cond3;
};


struct Trace_Entry{
    unsigned int DevID;
    long long StartByte;
    int ByteCount;
    char rwType;
    ull Request_us;
};

struct Record{
    ull ID;
    ull Request_us;
    ull Latency; //us
};

struct Queue_Entry{
    struct iocb IOcb;
    char *Buf;
    ull Request_us;
    ull ID;
    ull Addr;
    struct Queue_Entry *next;
};


//********************************************************


//********* Global Variences ************

#define NR_ARG 7
#define MAX_LINE_LENGTH 201
#define QUEUE_LENGTH 4096
#define AIO_MAXIO 512
#define IO_BUFFER_SIZE (512*256)
#define TRACE_BUFFER_SIZE 20000 

struct Config config;
//****************************************

static inline ull elapse_us(){
    struct timeval Current_tv;
    gettimeofday(&Current_tv,NULL);
    return Current_tv.tv_sec*1000000+Current_tv.tv_usec;
}


void* IO_Completion_Handler(void *thread_data){
    struct io_event events[AIO_MAXIO];
    struct Queue_Entry *this_io;
    int retIOs,i;
    ull n;
    ull return_us;
    ull ret1half=0,ret2half=0;
    while(1){
        if(config.should_stop && config.Nr_Flight_IOs==0 ){
                pthread_mutex_lock(&config.mutex3);
                pthread_cond_signal(&config.cond3);
                pthread_mutex_unlock(&config.mutex3);
		break;
	}
        retIOs=io_getevents(config.ctx,1,1,events,NULL);
        return_us=elapse_us();
        for(i=0;i<retIOs;i++){
            this_io=(struct Queue_Entry*)events[i].data;
            if(events[i].res2!=0){
                printf("AIO ERROR\n");
            }
            if(events[i].obj!=&this_io->IOcb){
                printf("IOCB LOST\n");
            }
            n=this_io->ID%TRACE_BUFFER_SIZE;
	    config.Record_Buffer[n].ID=this_io->Addr;
            config.Record_Buffer[n].Request_us=this_io->Request_us;
            config.Record_Buffer[n].Latency=return_us-this_io->Request_us;
            pthread_spin_lock(&config.spinlock);
            config.Queue_Free++;
            config.tail->next=this_io;
            config.tail=this_io;
            config.Nr_Flight_IOs--;
            pthread_spin_unlock(&config.spinlock);
            if(n<TRACE_BUFFER_SIZE/2)ret1half++;
            else ret2half++;
            if(config.Firsthalf&&config.Buffer_Needed){
                if(ret1half==TRACE_BUFFER_SIZE/2){
		    pr_debug("result signal send %d\n",config.Firsthalf);
                    ret1half=0;
                    pthread_mutex_lock(&config.mutex3);
                    pthread_cond_signal(&config.cond3);
                    pthread_mutex_unlock(&config.mutex3);
                }
            }else{
                if(ret2half==TRACE_BUFFER_SIZE/2){
		    pr_debug("result signal send %d\n",config.Firsthalf);
                    ret2half=0;
                    pthread_mutex_lock(&config.mutex3);
                    pthread_cond_signal(&config.cond3);
                    pthread_mutex_unlock(&config.mutex3);
                }
            }
	    //usleep(10);
            if(config.sync){
                pthread_mutex_lock(&config.mutex);
                pthread_cond_signal(&config.cond);
		pr_debug("signal:%llu\n",this_io->ID);
                pthread_mutex_unlock(&config.mutex);
            }
            //pr_debug("ID:%d,req_us:%llu,ret_us:%llu,latency:%llu flight:%d\n",id,this_io->Request_us,return_us,return_us-this_io->Request_us,config.Nr_Flight_IOs);
        }
    }
    pthread_exit(NULL);
}

void* Trace_Buffer_Manage_Handler(void *thread_data){
    int j,base;
    char line[MAX_LINE_LENGTH];
    double req_sec;
    while(1){
	j=0;
        if(config.should_stop)break;
        pthread_mutex_lock(&config.mutex2);
        pthread_cond_wait(&config.cond2,&config.mutex2);
        pthread_mutex_unlock(&config.mutex2);
        if(config.should_stop)break;
	pr_debug("trace signal received %d half\n",config.Firsthalf);
        if(config.Firsthalf)base=0;
        else base=TRACE_BUFFER_SIZE/2;
        while(j<TRACE_BUFFER_SIZE/2&&fgets(line,MAX_LINE_LENGTH,config.TraceFile)!=NULL){
            if(sscanf(line,"%u %lld %d %c %lf\n",&config.Trace_Buffer[base+j].DevID,&config.Trace_Buffer[base+j].StartByte,&config.Trace_Buffer[base+j].ByteCount,&config.Trace_Buffer[base+j].rwType,&req_sec)!=5){
                printf("Wrong Arguments for Trace\n");
            }
            config.Trace_Buffer[base+j].Request_us=(ull)(req_sec*1000000);
            config.Trace_Buffer[base+j].Request_us-=config.First_Request_us;
            j++;
        }
        config.Nr_Trace_Read+=j;
        if(j==TRACE_BUFFER_SIZE/2)config.Buffer_Needed=1;
	else config.Buffer_Needed=0;
    }
}

void* Result_Buffer_Manage_Handler(void *thread_data){
    int i,base;
    char line[MAX_LINE_LENGTH];
    double req_sec;
    while(1){
	if(config.should_stop)break;
        pthread_mutex_lock(&config.mutex3);
        pthread_cond_wait(&config.cond3,&config.mutex3);
        pthread_mutex_unlock(&config.mutex3);
        if(config.should_stop)break;
	pr_debug("result signal received %d half\n",config.Firsthalf);
        if(config.Firsthalf)base=0;
        else base=TRACE_BUFFER_SIZE/2;
        for(i=0;i<TRACE_BUFFER_SIZE/2;i++){
            fprintf(config.ResultFile, "%llu %llu\n", config.Record_Buffer[base+i].Request_us,config.Record_Buffer[base+i].Latency);
        }
        config.persisted+=TRACE_BUFFER_SIZE/2;
        config.Firsthalf=1-config.Firsthalf;
    }
}

static void finalize(){
    int i;
    if(config.Device>=0)close(config.Device);
    if(config.TraceFile!=NULL)fclose(config.TraceFile);
    if(config.ResultFile!=NULL)fclose(config.ResultFile);
    for( i=0 ; i<QUEUE_LENGTH ; i++ ){
        if(config.IOQueue[i].Buf!=NULL)free(config.IOQueue[i].Buf);
    }
    if(config.IOQueue!=NULL)free(config.IOQueue);
    if(config.Trace_Buffer!=NULL)free(config.Trace_Buffer);
    if(config.Record_Buffer!=NULL)free(config.Record_Buffer);
    io_destroy(config.ctx);
}

static int initialize(const char* Dev_Path, const char* Trace_Path, const char* Result_Path){
    int i,j=0;
    char line[MAX_LINE_LENGTH];
    double req_sec;

    pthread_spin_init(&config.spinlock,0);
    pthread_mutex_init(&config.mutex,NULL);  
    pthread_cond_init(&config.cond,NULL);
    pthread_mutex_init(&config.mutex2,NULL);  
    pthread_cond_init(&config.cond2,NULL);
    pthread_mutex_init(&config.mutex3,NULL);  
    pthread_cond_init(&config.cond3,NULL);
    config.Nr_Trace_Read    =   0;
    config.Nr_Flight_IOs    =   0;
    config.First_Entry_Flag =   1;
    config.should_stop  =   0;
    config.persisted = 0;
    if((config.Device=open(Dev_Path,O_RDWR|O_DIRECT))<0){
        printf("Device %s Open Failed\n",Dev_Path);
        goto Error;
    }
    if((config.TraceFile=fopen(Trace_Path,"rt"))==NULL){
        printf("TraceFile %s Open Failed\n",Trace_Path);
        goto Error;
    }
    if((config.ResultFile=fopen(Result_Path,"w"))==NULL){
        printf("ResultFile %s Open Failed\n",Result_Path);
        goto Error;
    }
    if(pthread_create(&config.Reap_th,NULL,IO_Completion_Handler,(void*)NULL)!=0){
        printf("Reap Thread Create Failed\n");
        goto Error;
    }
    if((config.IOQueue=malloc(sizeof(struct Queue_Entry)*QUEUE_LENGTH))==NULL){
        printf("Malloc IOQueue Failed\n");
        goto Error;
    }
    for( i=0 ; i<QUEUE_LENGTH ; i++ ){
        memset(&config.IOQueue[i],0,sizeof(struct Queue_Entry));
        if((posix_memalign((void **)&config.IOQueue[i].Buf,512,IO_BUFFER_SIZE))<0){
            printf("POSIX_MEMALIGN buffer %d failed\n",i);
            goto Error;
        }
        if(i<QUEUE_LENGTH-1)config.IOQueue[i].next=&config.IOQueue[i+1];
    }
    config.head=&config.IOQueue[0];
    config.tail=&config.IOQueue[QUEUE_LENGTH-1];
    config.Queue_Free=QUEUE_LENGTH;

    if((config.Trace_Buffer=malloc(sizeof(struct Trace_Entry)*TRACE_BUFFER_SIZE))==NULL){
        printf("Malloc Trace_Buffer failed\n");
        goto Error;
    }

    while(j<TRACE_BUFFER_SIZE&&fgets(line,MAX_LINE_LENGTH,config.TraceFile)!=NULL){
        if(sscanf(line,"%u %lld %d %c %lf\n",&config.Trace_Buffer[j].DevID,&config.Trace_Buffer[j].StartByte,&config.Trace_Buffer[j].ByteCount,&config.Trace_Buffer[j].rwType,&req_sec)!=5){
            printf("Wrong Arguments for Trace\n");
        }
        config.Trace_Buffer[j].Request_us=(ull)(req_sec*1000000);
        config.Trace_Buffer[j].Request_us-=config.Trace_Buffer[0].Request_us;
        j++;
    }
    config.First_Request_us=config.Trace_Buffer[0].Request_us;
    config.Nr_Trace_Read=j;
    config.Firsthalf=1;
    if(j==TRACE_BUFFER_SIZE)config.Buffer_Needed=1;
    else config.Buffer_Needed=0;


    if((config.Record_Buffer=malloc(sizeof(struct Record)*TRACE_BUFFER_SIZE))==NULL){
        printf("Malloc Record_Buffer failed\n");
        goto Error;
    }

    if(pthread_create(&config.TBufferManage_th,NULL,Trace_Buffer_Manage_Handler,(void*)NULL)!=0){
        printf("Buffer_Manage Thread Create Failed\n");
        goto Error;
    }
    if(pthread_create(&config.RBufferManage_th,NULL,Result_Buffer_Manage_Handler,(void*)NULL)!=0){
        printf("Buffer_Manage Thread Create Failed\n");
        goto Error;
    }

    config.ctx=0;
    if(io_setup(QUEUE_LENGTH,&config.ctx)){
        printf("io_setup Failed\n");
        goto Error;
    }

    return 0;
Error:
    finalize();
    return -1;
}


static int process_one_request(ull id){
    pr_debug("process %llu start\n",id);
    struct Queue_Entry *this_entry;
    struct iocb *this_iocb;
    int qfree;
    ull cur_us;
    ull n=id%TRACE_BUFFER_SIZE;
repeat:
    pthread_spin_lock(&config.spinlock);
    qfree=config.Queue_Free;
    if(config.Queue_Free>0){
        this_entry=config.head;
        config.head=this_entry->next;
        config.Queue_Free--;
        if(config.head==NULL&&qfree<=0){
            printf("Unexpected Situation in IOQueue Buffer\n");
        }
        config.Nr_Flight_IOs++;
    }
    pthread_spin_unlock(&config.spinlock);
    if(qfree<=0)goto repeat;
    //printf("process id:%d\n",id);
    this_entry->ID = id;
    this_entry->Addr = config.Trace_Buffer[n].StartByte;
    this_iocb=&this_entry->IOcb;
    if(config.Trace_Buffer[n].ByteCount>IO_BUFFER_SIZE){
        free(this_entry->Buf);
        if((posix_memalign((void **)this_entry->Buf,512,config.Trace_Buffer[n].ByteCount))<0){
            printf("RePOSIX_MEMALIGN buffer %llu failed\n",n);
        }
    }
    if(config.Trace_Buffer[n].rwType=='R'||config.Trace_Buffer[n].rwType=='r'){
        io_prep_pread(this_iocb,config.Device,this_entry->Buf,config.Trace_Buffer[n].ByteCount,config.Trace_Buffer[n].StartByte);
    }else{
        io_prep_pwrite(this_iocb,config.Device,this_entry->Buf,config.Trace_Buffer[n].ByteCount,config.Trace_Buffer[n].StartByte);
    }
    this_iocb->data=this_entry;
    cur_us=elapse_us();
    if(config.First_Entry_Flag){
        config.First_Entry_Flag=0;
        config.Trace_Start_us=cur_us;
    }
    if(config.Trace_Buffer[n].Request_us > cur_us-config.Trace_Start_us){
        if(!config.sync){
            usleep(config.Trace_Buffer[n].Request_us+config.Trace_Start_us-cur_us);
        }
    }
    this_entry->Request_us=elapse_us();
    if(io_submit(config.ctx,1,&this_iocb)<0){
        printf("io_submit Failed\n");
    }
    pr_debug("process %llu end\n",id);
}

static void trace_play(){
    ull n=0,i=0,j;
    ull now_us;
    while(1){
        process_one_request(n++);
	if(config.sync){
            pthread_mutex_lock(&config.mutex);
	    pr_debug("lock %llu\n",n-1);
            pthread_cond_wait(&config.cond,&config.mutex);
            pthread_mutex_unlock(&config.mutex);
	    pr_debug("unlock %llu\n",n-1);
        }
        i++;
        now_us=elapse_us();
        if(now_us-config.Trace_Start_us>config.Test_Time*1000000||n>=config.Nr_Trace_Read){
	    //printf("nr_trace_read:%d\n",config.Nr_Trace_Read);
	    config.played=n;
            config.should_stop=1;
            pthread_mutex_lock(&config.mutex2);
            pthread_cond_signal(&config.cond2);
            pthread_mutex_unlock(&config.mutex2);
            break;
        }
        if(i>=TRACE_BUFFER_SIZE/2&&config.Buffer_Needed){
	    pr_debug("trace signal\n");
            config.Buffer_Needed=0;
            i=0;
            pthread_mutex_lock(&config.mutex2);
            pthread_cond_signal(&config.cond2);
            pthread_mutex_unlock(&config.mutex2);
        }
        usleep(config.sleep_us);
    }
}

static void result_persist(){
    int i,start,end;
    if(config.played>config.persisted){
	start=config.persisted%TRACE_BUFFER_SIZE;
	end=config.played%TRACE_BUFFER_SIZE;
	printf("start:%d,end:%d\n",start,end);
	if(start<end){
		for(i=start;i<end;i++){
		fprintf(config.ResultFile,"%llu %llu\n", config.Record_Buffer[i].Request_us,config.Record_Buffer[i].Latency);
		}
	}else{
		for(i=start;i<start+TRACE_BUFFER_SIZE/2;i++){
		fprintf(config.ResultFile,"%llu %llu\n", config.Record_Buffer[i].Request_us,config.Record_Buffer[i].Latency);
		}
		for(i=0;i<end;i++){
		fprintf(config.ResultFile,"%llu %llu\n", config.Record_Buffer[i].Request_us,config.Record_Buffer[i].Latency);
		}
	}
    }
}


int main(int argc, char *argv[]){
    if(argc!=NR_ARG){
        printf("Argc=%d, Require argc=%d\n",argc,NR_ARG);
        return -1;
    }
    const char * Dev_Path=argv[1];
    const char * Trace_Path=argv[2];
    const char * Result_Path=argv[3];
    config.Test_Time = atoi(argv[4]);
    config.sync = atoi(argv[5]);
    config.sleep_us=atoi(argv[6]);    
    if(initialize(Dev_Path,Trace_Path,Result_Path)<0)return -1;
    trace_play();
    pthread_join(config.Reap_th,NULL);
    pr_debug("reap_th join\n");
    pthread_join(config.TBufferManage_th,NULL);
    pr_debug("tbuff_th join\n");
    pthread_join(config.RBufferManage_th,NULL);
    pr_debug("rbuff_th join\n");
    result_persist();
    finalize();
    return 0;
}
