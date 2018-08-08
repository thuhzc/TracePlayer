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

struct Node
{
	struct Node *prev, *next;
	sector_t key;
};

struct LRUCache
{
	sector_t LRU_size, cnt;
	struct Node *head, *tail, *free, *pool;
};

struct zombie{
	spinlock_t lock;
	struct request_queue *queue;
	struct gendisk *gd;
	struct list_head disks;
	sector_t capacity;
	int n_subdev;
	struct bio_set *bio_set;
	int mirror_ratio;
	sector_t mirror_chunks;
	sector_t cur_mirror;
	sector_t raid_chunks;
	struct block_device *bdevs;
	struct LRUCache *lru;
	struct task_struct *tsk;
};

struct subdev{
	struct block_device *bdev;
	int id;
	sector_t size;
	struct list_head same_set;
};

void LRU_init( struct zombie *dev, sector_t LRU_size )
{
	sector_t i;
	dev->lru = ( struct LRUCache *)kmalloc( sizeof( struct LRUCache ),GFP_KERNEL );
	dev->lru->LRU_size = LRU_size;
	dev->lru->cnt = 0;
	dev->lru->head = dev->lru->tail = dev->lru->free = NULL;
	dev->lru->pool = ( struct Node * )vmalloc( sizeof( struct Node )*LRU_size );
	for( i = 0; i < dev->lru->LRU_size; i ++ ){
		struct Node* now = &dev->lru->pool[i];
		now->next = dev->lru->free;
		now->prev = NULL;
		now->key = i;
		if( dev->lru->free != NULL ){
			dev->lru->free->prev = now;
		}
		dev->lru->free = now;
	}
}

void setHead( struct zombie *dev, Node* now )
{
	now->next = dev->lru->head;
	now->prev = NULL;

	if (dev->lru->head != NULL){
		dev->lru->head->prev = now;
	}
	dev->lru->head = now;
	if (dev->lru->tail == NULL){
		dev->lru->tail = dev->lru->head;
	}
}

void remove(struct zombie *dev, Node *node)
{
	if (node->prev != NULL){
		node->prev->next = node->next;
	}
	else{
		dev->lru->head = node->next;
	}
	if (node->next != NULL){
		node->next->prev = node->prev;
	}
	else{
		dev->lru->tail = node->prev;
	}
}

sector_t set(struct zombie *dev, sector_t key)
{
	struct Node* tmp;
	if( key == -1 ){
		if( dev->lru->cnt >= dev->lru->LRU_size/2 ){//change threshold
			sector_t T = i3;//num of remove
			while( T -- ){
				if( dev->lru->cnt <= 0 ) break;
				struct Node* now = dev->lru->tail;
				remove(now);
				//reback dev->lru node
				now->next = dev->lru->free;
				now->prev = NULL;
				if( dev->lru->free != NULL ) dev->lru->free->prev = now;
				dev->lru->free = now;
				dev->lru->cnt --;
			}
		}
		//select one from freelist
		tmp = dev->lru->free;
		dev->lru->free = tmp->next;
		if( tmp->next != NULL ){
			tmp->next->prev = tmp->prev;
		}
		dev->lru->cnt ++;
	}
	else{
		tmp = &dev->lru->pool[key];
		remove(tmp);
	}
	setHead(tmp);
	return tmp->key;
}

//major device number
static int z_major=0;

//zombie instance
struct zombie *zombie_i;

static void zombie_endio(struct bio *bi)
{

}

//device make request
static int zombie_request(struct zombie *dev, struct bio * bio)
{
	sector_t bio_sector,chunk_sector,sectors;
	bio_sector = bio->bi_iter.bi_sector;
	chunk_sector = dev->chunk_sectors;
	sectors = chunk_sector - (bio_sector & (chunk_sector-1));
	if (sectors < bio_sector(bio)){
		struct bio *split = bio_split(bio, sectors, GFP_NOIO, dev->bio_set);
		bio_chain(split, bio);
		generic_make_request(bio);
		bio = split;
	}
	int disk, i;
	sector_t vchunk;
	sector_t lchunk = bio->bi_iter.bi_sector/dev->chunk_sectors;
	sector_t value = dev->mapping_table[lchunk];
	int valid = value >> 63;
	int mirror = (value >> 62)&1;
	const int rw = bio_data_dir(bio);
	if(rw == WRITE){
		if(!valid){
			vchunk = lru_set(dev, -1);
			dev->mappint_table[lchunk] = (1LL<<63)|(1LL<<62)|vchunk;
			bio->bi_iter.bi_sector = vchunk*2/dev->n_subdev;
			disk = vchunk%dev->n_subdev;
			bio->bi_bdev = dev->bdevs[disk];
		}
		else{
			if(mirror){
				vchunk = ((value & ((1LL<<62)-1))-1)*2;
				bio->bi_iter.bi_sector = vchunk/dev->n_subdev;
				disk = vchunk%dev->n_subdev;
				bio->bi_bdev = dev->bdevs[disk];
				lru_set(dev, vchunk);
			}else{
				if(bio_sector(bio) == chunk_sector){
					vchunk = lru_set(dev, -1);
					dev->mappint_table[lchunk] = (1LL<<63)|(1<<62)|vchunk;
					bio->bi_iter.bi_sector = vchunk*2/dev->n_subdev;
					disk = vchunk%dev->n_subdev;
					bio->bi_bdev = dev->bdevs[disk];
				}else{
					struct bio *rbio = bio_alloc_bioset(GFP_NOIO,dev->chunk_pages,dev->bio_set);
					if(!rbio){
						printk("alloc bio failed\n");
						return -1;
					}
					rbio->bi_private = bio;
					vchunk = ((value & ((1LL<<62)-1))-1) - dev->mirror_chunks;
					sector_t sector = vchunk/ (dev->n_subdev-1);
					disk = vchunk%(dev->n_subdev-1);
					int parity = sector%dev->n_subdev;
					if(disk>=parity)disk++;
					rbio->bi_iter.bi_sector = sector;
					rbio->bi_bdev = dev->bdevs[disk];
					rbio->bi_endio = preread_for_paritialwrite_endio;
					bio_set_op_attrs(rbio,REQ_OP_READ,0);
					rbio->bi_vcnt = dev->chunk_pages;
				}
			}
		}
	}
	if(rw == READ){
		if(!valid)return -1;
		if(mirror){
			vchunk = ((value & ((1LL<<62)-1))-1)*2;
			bio->bi_iter.bi_sector = vchunk/dev->n_subdev;
			disk = vchunk%dev->n_subdev;
			bio->bi_bdev = dev->bdevs[disk];
			lru_set(dev, vchunk);
		}
		else {
			vchunk = ((value & ((1LL<<62)-1))-1) - dev->mirror_chunks;
			sector_t sector = vchunk/ (dev->n_subdev-1);
			disk = vchunk%(dev->n_subdev-1);
			int parity = sector%dev->n_subdev;
			if(disk>=parity)disk++;
			bio->bi_iter.bi_sector = sector;
			bio->bi_bdev = dev->bdevs[disk];
		}
	}
	generic_make_request(bio);

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
	return 0;
}

static int run_array(struct zombie *dev, void __user *arg)
{
	dev->lru = ( struct LRUCache *)kmalloc( sizeof( struct LRUCache ), GFP_KERNEL );	
	dev->tsk = 
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
		case 0x02:
			run_array(dev,argp);
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
	zombie_i->capacity=0;
	z_major=register_blkdev(z_major,"zombie");
	if(z_major<=0){
		printk(KERN_WARNING "zombie:unable to get major number\n");
		return -EBUSY;
	}
	zombie_i->queue=blk_alloc_queue(GFP_KERNEL);
	if(zombie_i->queue==NULL)return -ENOMEM;
	blk_queue_make_request(zombie_i->queue,zombie_make_request);
	zombie_i->queue->queuedata = zombie_i;	
	zombie_i->gd=alloc_disk(1);
	if(!zombie_i->gd){
		printk("alloc_disk failed\n");
		return -ENOMEM;
	}

	zombie_i->gd->major=z_major;
	zombie_i->gd->first_minor=0;
	zombie_i->gd->fops=&zombie_ops;
	zombie_i->gd->queue=zombie_i->queue;
	zombie_i->gd->private_data = zombie_i;
	strcpy(zombie_i->gd->disk_name,"zombie");
	set_capacity(zombie_i->gd,128);//512Byte unit
	add_disk(zombie_i->gd);
	if(zombie_i->bio_set==NULL){
		zombie_i->bio_set=bioset_create(BIO_POOL_SIZE,0,0);
		if(!zombie_i->bio_set)return -ENOMEM;
	}	

	printk("Zombie: installed\n");
	return 0;
}

static void zombie_exit(void)
{
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
	if(zombie_i->bio_set)bioset_free(zombie_i->bio_set);
	unregister_blkdev(z_major,"zombie");
	kfree(zombie_i);
}

module_init(zombie_init);
module_exit(zombie_exit);

