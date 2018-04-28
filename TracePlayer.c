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

#define pr_debug(fmt, args...) printf("[%s() L.%d]: "fmt, __func__, __LINE__, ## args)

typedef unsigned long long ull;

//************* Data Structure Definition ****************

struct Trace_Entry{
    unsigned int DevID;
    long long StartByte;
    int ByteCount;
    char rwType;
    ull Request_us;
};

struct Record{
    ull Request_us;
    ull Latency; //us
};

struct Queue_Entry{
    struct iocb IOcb;
    char *Buf;
    ull Request_us;
    int ID;
    struct Queue_Entry *next;
};

//********************************************************


//********* Global Variences ************


#define NR_ARG 5
#define MAX_LINE_LENGTH 201
#define QUEUE_LENGTH 4096
#define AIO_MAXIO 512
//#define IO_BUFFER_SIZE 4096
#define IO_BUFFER_SIZE (512*256)
#define TRACE_BUFFER_SIZE 100000 

struct timeval Start_tv;
int Device;
FILE *Trace, *Results;
pthread_t Reap_th;
pthread_t BM_th;
struct Queue_Entry *IOQueue;
struct Queue_Entry *head, *tail;
int Queue_Free;
struct Trace_Entry *Trace_Buffer;
struct Record *Record_Buffer;
io_context_t ctx;
ull Trace_Start_us;
ull Nr_Trace_Read;
int should_stop;
int Nr_Flight_IOs;
pthread_spinlock_t spinlock;
int First_Entry_Flag;
int Test_Sec;
ull total_sleep;
ull sub1,sub2,sub3,reap1,reap2,reap3;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;  
pthread_cond_t  cond  = PTHREAD_COND_INITIALIZER; 
//****************************************

static inline ull elapse_us(){
    struct timeval Current_tv;
    gettimeofday(&Current_tv,NULL);
    return (Current_tv.tv_sec-Start_tv.tv_sec)*1000000+(Current_tv.tv_usec-Start_tv.tv_usec);
}


void* IO_Completion_Handler(void *thread_data){
    struct io_event events[AIO_MAXIO];
    struct Queue_Entry *this_io;
    int retIOs,i,id;
    ull return_us;
    ull t1,t2;
    while(1){
        if(should_stop && Nr_Flight_IOs==0 )break;
        retIOs=io_getevents(ctx,1,1,events,NULL);
        return_us=elapse_us();
        //if(retIOs>1)pr_debug("more than 1 IO return:%d \n",retIOs);
        for(i=0;i<retIOs;i++){
            this_io=(struct Queue_Entry*)events[i].data;
            if(events[i].res2!=0){
                printf("AIO ERROR\n");
            }
            if(events[i].obj!=&this_io->IOcb){
                printf("IOCB LOST\n");
            }
            id=this_io->ID;
            Record_Buffer[id].Request_us=this_io->Request_us;
            Record_Buffer[id].Latency=return_us-this_io->Request_us;
            t1=elapse_us();
            pthread_spin_lock(&spinlock);
            Queue_Free++;
            tail->next=this_io;
            tail=this_io;
            Nr_Flight_IOs--;
            pthread_spin_unlock(&spinlock);
            t2=elapse_us();
            reap1+=t2-t1;
            
            pthread_mutex_lock(&mutex);
            pthread_cond_signal(&cond);
            pthread_mutex_unlock(&mutex);
            
            pr_debug("ID:%d,req_us:%llu,ret_us:%llu,latency:%llu flight:%d should_stop:%d\n",id,this_io->Request_us,return_us,return_us-this_io->Request_us,Nr_Flight_IOs,should_stop);
        }
    }
    pthread_exit(NULL);
}

void* Buffer_Manage_Handler(void *thread_data){
}

static void finalize(){
    pr_debug("finalize\n");
    int i;
    ull cur_us;
    if(Device>=0)close(Device);
    if(Trace!=NULL)fclose(Trace);
    if(Results!=NULL)fclose(Results);
    for( i=0 ; i<QUEUE_LENGTH ; i++ ){
        if(IOQueue[i].Buf!=NULL)free(IOQueue[i].Buf);
    }
    if(IOQueue!=NULL)free(IOQueue);
    if(Trace_Buffer!=NULL)free(Trace_Buffer);
    if(Record_Buffer!=NULL)free(Record_Buffer);
    io_destroy(ctx);
}

static int initialize(const char* Dev_Path, const char* Trace_Path, const char* Result_Path){
    pr_debug("Initialize\n");
 
    int i,j=0;
    char line[MAX_LINE_LENGTH];
    double req_sec;

    pthread_spin_init(&spinlock,0);
    Nr_Trace_Read=0;
    Nr_Flight_IOs=0;
    First_Entry_Flag=1;
    should_stop=0;
    if((Device=open(Dev_Path,O_RDWR|O_DIRECT))<0){
        printf("Device %s Open Failed\n",Dev_Path);
        goto Error;
    }
    if((Trace=fopen(Trace_Path,"rt"))==NULL){
        printf("TraceFile %s Open Failed\n",Trace_Path);
        goto Error;
    }
    if((Results=fopen(Result_Path,"w"))==NULL){
        printf("ResultFile %s Open Failed\n",Result_Path);
        goto Error;
    }
    if(pthread_create(&Reap_th,NULL,IO_Completion_Handler,(void*)NULL)!=0){
        printf("Reap Thread Create Failed\n");
        goto Error;
    }
    if((IOQueue=malloc(sizeof(struct Queue_Entry)*QUEUE_LENGTH))==NULL){
        printf("Malloc IOQueue Failed\n");
        goto Error;
    }
    for( i=0 ; i<QUEUE_LENGTH ; i++ ){
        memset(&IOQueue[i],0,sizeof(struct Queue_Entry));
        if((posix_memalign((void **)&IOQueue[i].Buf,512,IO_BUFFER_SIZE))<0){
            printf("POSIX_MEMALIGN buffer %d failed\n",i);
            goto Error;
        }
        if(i<QUEUE_LENGTH-1)IOQueue[i].next=&IOQueue[i+1];
    }
    head=&IOQueue[0];
    tail=&IOQueue[QUEUE_LENGTH-1];
    Queue_Free=QUEUE_LENGTH;

    if((Trace_Buffer=malloc(sizeof(struct Trace_Entry)*TRACE_BUFFER_SIZE))==NULL){
        printf("Malloc Trace_Buffer failed\n");
        goto Error;
    }
    while(fgets(line,MAX_LINE_LENGTH,Trace)!=NULL&j<TRACE_BUFFER_SIZE){
        if(sscanf(line,"%u %lld %d %c %lf\n",&Trace_Buffer[i].DevID,&Trace_Buffer[j].StartByte,&Trace_Buffer[j].ByteCount,&Trace_Buffer[j].rwType,&req_sec)!=5){
            printf("Wrong Arguments for Trace\n");
        }
        Trace_Buffer[j].Request_us=(ull)(req_sec*1000000);
        Trace_Buffer[j].Request_us-=Trace_Buffer[0].Request_us;
        j++;
    }
    Nr_Trace_Read=j;


    if((Record_Buffer=malloc(sizeof(struct Record)*TRACE_BUFFER_SIZE))==NULL){
        printf("Malloc Record_Buffer failed\n");
        goto Error;
    }

    if(pthread_create(&BM_th,NULL,Buffer_Manage_Handler,(void*)NULL)!=0){
        printf("Buffer_Manage Thread Create Failed\n");
        goto Error;
    }

    //ctx=0;
    memset(&ctx,0,sizeof(ctx));
    if(io_setup(QUEUE_LENGTH,&ctx)){
        printf("io_setup Failed\n");
        goto Error;
    }
    total_sleep=0;
    sub1=0;
    sub2=0;
    sub3=0;
    reap1=0;
    return 0;
Error:
    finalize();
    return -1;
}


static int process_one_request(int n){
    struct Queue_Entry *this_entry;
    struct iocb *this_iocb;
    int qfree;
    ull cur_us;
    ull t1,t2;
    t1=elapse_us();
repeat:
    pthread_spin_lock(&spinlock);
    qfree=Queue_Free;
    if(Queue_Free>0){
        this_entry=head;
        head=this_entry->next;
        Queue_Free--;
        if(head==NULL&&qfree<=0){
            printf("Unexpected Situation in IOQueue Buffer\n");
        }
        Nr_Flight_IOs++;
    }
    pthread_spin_unlock(&spinlock);
    if(qfree<=0)goto repeat;
    t2=elapse_us();
    sub1+=t2-t1;
    this_entry->ID = n;
    this_iocb=&this_entry->IOcb;
    if(Trace_Buffer[n].ByteCount>IO_BUFFER_SIZE){
        free(this_entry->Buf);
        if((posix_memalign((void **)this_entry->Buf,512,Trace_Buffer[n].ByteCount))<0){
            printf("RePOSIX_MEMALIGN buffer %d failed\n",n);
        }
    }
    t1=elapse_us();
    if(Trace_Buffer[n].rwType=='R'||Trace_Buffer[n].rwType=='r'){
        io_prep_pread(this_iocb,Device,this_entry->Buf,Trace_Buffer[n].ByteCount,Trace_Buffer[n].StartByte);
    }else{
        io_prep_pwrite(this_iocb,Device,this_entry->Buf,Trace_Buffer[n].ByteCount,Trace_Buffer[n].StartByte);
    }
    this_iocb->data=this_entry;
    cur_us=elapse_us();
    sub2+=cur_us-t1;
    if(First_Entry_Flag){
        First_Entry_Flag=0;
        Trace_Start_us=cur_us;
    }
    if(Trace_Buffer[n].Request_us > cur_us-Trace_Start_us){
        total_sleep+=Trace_Buffer[n].Request_us+Trace_Start_us-cur_us;
        //usleep(Trace_Buffer[n].Request_us+Trace_Start_us-cur_us);
    }
    t1=elapse_us();
    this_entry->Request_us=elapse_us();
    if(io_submit(ctx,1,&this_iocb)<0){
        printf("io_submit Failed\n");
    }
    t2=elapse_us();
    sub3+=t2-t1;
}

static void trace_play(){
    pr_debug("Start trace_play\n");
    int n=0;
    ull now_us;
    while(1){
        now_us=elapse_us();
        if(now_us>Test_Sec*1000000||n>=Nr_Trace_Read){
            should_stop=1;
            break;
        }
        process_one_request(n++);
        pthread_mutex_lock(&mutex);
        pthread_cond_wait(&cond,&mutex);
        pthread_mutex_unlock(&mutex);
        usleep(50);
    }
    pr_debug("avg sleep:%llu, %d\n",total_sleep/(n-1),n);
}

static void result_persist(){
    pr_debug("result_persist\n");
    int i;
    for(i=0;i<Nr_Trace_Read;i++){
        fprintf(Results, "%llu %llu\n", Record_Buffer[i].Request_us,Record_Buffer[i].Latency );
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
    Test_Sec= atoi(argv[4]);
    if(initialize(Dev_Path,Trace_Path,Result_Path)<0)return -1;
    gettimeofday(&Start_tv,NULL);
    trace_play();
    pthread_join(Reap_th,NULL);
    result_persist();
    finalize();
    pr_debug("sub1:%llu,sub2:%llu,sub3:%llu,reap1:%llu\n",sub1,sub2,sub3,reap1);
    return 0;
}
