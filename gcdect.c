//#include <linux/config.h>
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/init.h>
#include <linux/ioctl.h>
#include <linux/sched.h>
#include <linux/kernel.h>	/* printk() */
#include <linux/slab.h>		/* kmalloc() */
#include <linux/fs.h>		/* everything... */
#include <linux/errno.h>	/* error codes */
#include <linux/timer.h>
#include <linux/types.h>	/* size_t */
#include <linux/fcntl.h>	/* O_ACCMODE */
#include <linux/hdreg.h>	/* HDIO_GETGEO */
#include <linux/kdev_t.h>
#include <linux/vmalloc.h>
#include <linux/genhd.h>
#include <linux/blkdev.h>
#include <linux/buffer_head.h>	/* invalidate_bdev */
#include <linux/bio.h>
#include <linux/string.h>
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/sched_clock.h>
#include <linux/time.h>

MODULE_LICENSE("Dual BSD/GPL");

#define BLK_SIZE_BITS 9
#define TIME_TH 5 //ms
#define PENDING_TH 5 //number of pending requests exceeding TIME_TH ms
#define EPOCH_MS 200
#define POOL_SIZE 3000

struct dev_id{
	int major;
	int minor;
};


struct attach_data{
	struct bio *bi;
	int epoch;
	unsigned long start_us;
	struct subdev *dev;
};

struct unit{
	struct attach_data content;
	struct unit* next;
};

struct zombie{
	spinlock_t lock;
	struct request_queue *queue;
	struct gendisk *gd;
	struct list_head disks;
	sector_t size;
	int n_subdev;
	struct bio_set *bio_set;
	//struct unit *pool;
	//struct unit *head;
};

struct subdev{
	struct block_device *bdev;
	int id;
	sector_t size;
	struct list_head same_set;

	int *pending;
	unsigned long *index;
	int n_timeout; 
	spinlock_t lock;
};



//major device number
static int z_major=0;

static int timeout=0;

static unsigned long base_us;

struct unit* pool;
struct unit* head;
struct unit* tail;
static unsigned long free=POOL_SIZE;
spinlock_t mlock;



//zombie instance
struct zombie *zombie_i;

static void zombie_endio(struct bio *bi)
{
	struct unit *u=bi->bi_private;
	struct attach_data *data=&u->content;
	struct bio *origin_bi = data->bi;
	struct subdev *rdev=data->dev;
	int j;
	bio_put(bi);
	unsigned long now_epoch=(sched_clock()/1000-base_us)/EPOCH_MS;
	if(now_epoch-data->epoch>=TIME_TH){
		//spin_lock(&rdev->lock);
		rdev->n_timeout--;
		//spin_unlock(&rdev->lock);
	}else{
		j=data->epoch%TIME_TH;
		//spin_lock(&rdev->lock);
		rdev->pending[j]--;
		//spin_unlock(&rdev->lock);
	}
	
	//spin_lock(&mlock);
	free++;
	tail->next=u;
	tail=u;
	//spin_unlock(&mlock);
	
	if(!bi->bi_error){
		bio_endio(origin_bi);
		return;
	}
}

//device make request
static struct bio * zombie_request(struct zombie *dev, struct bio * bi)
{
	int i,j;
	struct bio* rbio;
	
	struct unit* u;
	//spin_lock(&mlock);
	if(free<=0){
		printk("Mempool run out\n");
		bio_endio(bi);
		//spin_unlock(&mlock);
		return NULL;
	}
	u=head;
	head=u->next;
	free--;
	//spin_unlock(&mlock);

	struct attach_data *data=&u->content;
	rbio=bio_clone_fast(bi,GFP_NOIO,dev->bio_set);
	rbio->bi_private = u;
	rbio->bi_end_io = zombie_endio;
	data->bi=bi;
	unsigned long now_us=sched_clock()/1000;
	data->epoch=(now_us-base_us)/EPOCH_MS;
	data->start_us=now_us;
	struct subdev *rdev;
	int disk=0;
	list_for_each_entry(rdev,&dev->disks,same_set){
		if(rdev->id==disk)break;
	}
	data->dev=rdev;
	for(i=0;i<TIME_TH;i++){
		if(rdev->index[i]!=0&&data->epoch-rdev->index[i]>=TIME_TH){
			//spin_lock(&rdev->lock);
			rdev->n_timeout+=rdev->pending[i];
			rdev->index[i]=0;
			rdev->pending[i]=0;
			//spin_unlock(&rdev->lock);
		}
	}

	if(rdev->n_timeout>PENDING_TH){
		timeout++;
		bio_endio(bi);
		return NULL;
	}
	else{
		rbio->bi_bdev=rdev->bdev;
		j=data->epoch%TIME_TH;
		//spin_lock(&rdev->lock);
		rdev->pending[j]++;
		rdev->index[j]=data->epoch; 
		//spin_unlock(&rdev->lock);
	}
	generic_make_request(rbio);
	return NULL;
}


//wrapped device make request
static blk_qc_t zombie_make_request(struct request_queue *q, struct bio *bi)
{
	struct zombie *dev = q->queuedata;
	zombie_request(dev,bi);
	return BLK_QC_T_NONE;
}

//assign block device to a subdev
static int lock_rdev(struct subdev *rdev, dev_t dev)
{
	int err = 0;
	struct block_device *bdev;
	bdev = blkdev_get_by_dev(dev, FMODE_READ|FMODE_WRITE|FMODE_EXCL,rdev);
	if(IS_ERR(bdev)){
		printk(KERN_WARNING "Can't open device:%d\n",dev);
		return PTR_ERR(bdev);
	}
	rdev->bdev=bdev;
	return err;	
}

static void unlock_rdev(struct subdev *rdev)
{
	struct block_device *bdev=rdev->bdev;
	rdev->bdev=NULL;
	blkdev_put(bdev, FMODE_READ|FMODE_WRITE|FMODE_EXCL);
	kfree(rdev);
}


static int add_new_device(struct zombie *dev, void __user *arg)
{
	struct subdev *rdev;
	int err;
	rdev=kzalloc(sizeof(*rdev),GFP_KERNEL);
	if(!rdev)return -EFAULT;
	struct dev_id did;
	if (copy_from_user(&did,arg,sizeof(struct dev_id)))return -EFAULT;
	dev_t new_dev=MKDEV(did.major,did.minor);
	err=lock_rdev(rdev,new_dev);
	if(err)return -1;
	rdev->size=i_size_read(rdev->bdev->bd_inode)>>BLK_SIZE_BITS;
	if(!rdev->size){
		return -EFAULT;
	}
	dev->size+=rdev->size;
	set_capacity(dev->gd,dev->size);
	revalidate_disk(dev->gd);
	rdev->id=dev->n_subdev++;
	list_add(&rdev->same_set,&dev->disks);

	rdev->pending=kzalloc(sizeof(int)*TIME_TH,GFP_KERNEL);	
	rdev->index=kzalloc(sizeof(int)*TIME_TH,GFP_KERNEL);
	spin_lock_init(&rdev->lock);	
	rdev->n_timeout=0;
	return 0;
}

static int zombie_ioctl(struct block_device *bdev, fmode_t mode, unsigned int cmd, unsigned long arg)
{
	void __user *argp = (void __user *)arg;
	struct zombie *dev=NULL;
	dev=bdev->bd_disk->private_data;
	int err;
	switch(cmd){
	case 0x01:
		err = add_new_device(dev,argp);
		if(err)printk("Failed to add device\n");
		break;	
	}
	return 0;
}

static int zombie_open(struct block_device *bdev, fmode_t mode)
{
	printk("Zombie device open\n");
	return 0;
}

static void zombie_release(struct gendisk *disk, fmode_t mode)
{
	printk("Zombie device release\n");
	return 0;
}

//device supporting operations
static struct block_device_operations zombie_ops = {
	.owner=THIS_MODULE,
	.open=zombie_open,
	.release=zombie_release,
	.ioctl=zombie_ioctl
};


static int __init zombie_init(void)
{
	zombie_i=kzalloc(sizeof(*zombie_i),GFP_KERNEL);
	INIT_LIST_HEAD(&zombie_i->disks);
	zombie_i->size=0;
	base_us=sched_clock()/1000;
	//apply for major number
	z_major=register_blkdev(z_major,"zombie");
	if(z_major<=0){
		printk(KERN_WARNING "zombie:unable to get major number\n");
		return -EBUSY;
	}
	//block device allocation and setup
	zombie_i->queue=blk_alloc_queue(GFP_KERNEL);
	if(zombie_i->queue==NULL)return -ENOMEM;
	blk_queue_make_request(zombie_i->queue,zombie_make_request);
	zombie_i->queue->queuedata = zombie_i;	
	zombie_i->gd=alloc_disk(1);
	if(!zombie_i->gd){
		printk("alloc_disk failed\n");
		return -ENOMEM;
	}

	//gendisk init
	zombie_i->gd->major=z_major;
	zombie_i->gd->first_minor=0;
	zombie_i->gd->fops=&zombie_ops;
	zombie_i->gd->queue=zombie_i->queue;
	zombie_i->gd->private_data = zombie_i;
	strcpy(zombie_i->gd->disk_name,"zombie");
	set_capacity(zombie_i->gd,128);//512Byte unit
	add_disk(zombie_i->gd);
	if(zombie_i->bio_set==NULL){
		zombie_i->bio_set=bioset_create(BIO_POOL_SIZE,0);
		if(!zombie_i->bio_set)return -ENOMEM;
	}	
	
	//allocate space for mempool and init
	printk("Size of unit:%d\n",sizeof(struct unit));
	pool=vmalloc(sizeof(struct unit)*POOL_SIZE);	
	head=&pool[0];
	tail=&pool[POOL_SIZE-1];
	free=POOL_SIZE;
	int i;
	for(i=0;i<POOL_SIZE-1;i++){
		pool[i].next=&pool[i+1];
	}
	spin_lock_init(&mlock);
	printk("Zombie: installed\n");
	return 0;
}

static void zombie_exit(void)
{
	printk("timeout:%d\n",timeout);
	struct subdev *rdev;
	while(!list_empty(&zombie_i->disks)){
		rdev=list_first_entry(&zombie_i->disks,struct subdev,same_set);
		kfree(rdev->pending);
		kfree(rdev->index);
		list_del(&rdev->same_set);
		unlock_rdev(rdev);
	}
	del_gendisk(zombie_i->gd);
	put_disk(zombie_i->gd);
	blk_cleanup_queue(zombie_i->queue);
	//blk_put_queue(zombie_i->queue);
	if(zombie_i->bio_set)bioset_free(zombie_i->bio_set);
	if(pool)vfree(pool);
	unregister_blkdev(z_major,"zombie");
	kfree(zombie_i);
	printk("Zombie: uninstalled\n");
}

module_init(zombie_init);
module_exit(zombie_exit);

