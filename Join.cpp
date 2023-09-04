#include "Join.hpp"

#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	// TODO: implement partition phase
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));


/*
RECORDS_PER_PAGE: the maximum number of records in one page
MEM_SIZE_IN_PAGE: the size of memory in units of page
DISK_SIZE_IN_PAGE: the size of disk in the units of page
*/
/*	Record.partition_hash(): returns a hash value (h1) 
for the key of the record. To build the 
in-memory hash table, you should do modulo 
(MEM_SIZE_IN_PAGE - 1) on this hash value.

Record.probe_hash(): returns a hash value (h2 
different from h1) for the key of the record. 
To build the in-memory hash table, you should 
do modulo (MEM_SIZE_IN_PAGE - 2) on this hash value.
Overloaded operator==: this equality operator 
checks whether the keys of two data records 
are the same or not. To make sure you use 
probe_hash() to speed up the probe phase, 
we will only allow equality comparison of 
two records with the same h2 hash value.*/
	mem->reset();

	uint beginL = left_rel.first; //disk page id begin
	uint endL = left_rel.second; //disk page id end
	uint beginR = right_rel.first;
	uint endR = right_rel.second;

	uint input_mem_page = MEM_SIZE_IN_PAGE - 1;
	Page* input_page = mem->mem_page(input_mem_page);

	uint current_mem_page = 0;
	Page* current_page = mem->mem_page(current_mem_page);


	for(uint lDiskPage = beginL; lDiskPage < endL; ++lDiskPage)
	{
		input_page->reset();
		mem->loadFromDisk(disk, lDiskPage, input_mem_page);

		for(uint lRecordId = 0; lRecordId < input_page->size(); ++lRecordId)
		{
			Record current_record = input_page->get_record(lRecordId);
			uint bucket_hash = current_record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			
			current_page = mem->mem_page(bucket_hash);

			current_page->loadRecord(current_record);
			
			if(current_page->full())
			{
				uint disk_write_to = mem->flushToDisk(disk, bucket_hash);
				partitions.at(bucket_hash).add_left_rel_page(disk_write_to);
				current_page->reset();
			}
			//partitions.at(bucket_hash).add_left_rel_page(lDiskPage);
		}

		//if(current_page->size() >= RECORDS_PER_PAGE)
		//if(current_page->empty())
		//if(current_page->full())

	}

	for(uint i = 0; i < MEM_SIZE_IN_PAGE - 1; ++i)
	{
		Page* to_check = mem->mem_page(i);

		if(!to_check->empty())
		{
			uint disk_write_to = mem->flushToDisk(disk, i);
			partitions.at(i).add_left_rel_page(disk_write_to);
			to_check->reset();
		}
	}

	/* reset all memory pages */
	mem->reset();


	for(uint rDiskPage = beginR; rDiskPage < endR; ++rDiskPage)
	{
		input_page->reset();
		mem->loadFromDisk(disk, rDiskPage, input_mem_page);

		for(uint rRecordId = 0; rRecordId < input_page->size(); ++rRecordId)
		{
			Record current_record = input_page->get_record(rRecordId);
			uint bucket_hash = current_record.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
			
			current_page = mem->mem_page(bucket_hash);

			current_page->loadRecord(current_record);
			
			if(current_page->full())
			{
				uint disk_write_to = mem->flushToDisk(disk, bucket_hash);
				partitions.at(bucket_hash).add_right_rel_page(disk_write_to);
				current_page->reset();
			}
			//partitions.at(bucket_hash).add_left_rel_page(lDiskPage);
		}

		//if(current_page->size() >= RECORDS_PER_PAGE)
		//if(current_page->empty())
		//if(current_page->full())

	}

	for(uint i = 0; i < MEM_SIZE_IN_PAGE - 1; ++i)
	{
		Page* to_check = mem->mem_page(i);

		if(!to_check->empty())
		{
			uint disk_write_to = mem->flushToDisk(disk, i);
			partitions.at(i).add_right_rel_page(disk_write_to);
			to_check->reset();
		}
	}

	/* reset all memory pages */
	mem->reset();


	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	vector<uint> disk_pages; // placeholder

	// STORE SMALLER RELATION INTO MEMORY
	// THEN READ LARGER ONE BY ONE TO THE INPUT BUFFER

	mem->reset();
	uint input_mem_page = MEM_SIZE_IN_PAGE - 1;
	Page* input_page = mem->mem_page(input_mem_page);
	uint output_mem_page = MEM_SIZE_IN_PAGE - 2;
	Page* output_page = mem->mem_page(output_mem_page);
	for(uint k = 0; k < partitions.size(); ++k)
	{
		for(uint re = 0; re < MEM_SIZE_IN_PAGE; ++re)
		{
			if (re == output_mem_page){
				continue;
			}
			mem->mem_page(re)->reset();
		}
		Bucket b = partitions.at(k);

		bool leftSmaller = b.num_left_rel_record <= b.num_right_rel_record;

		vector<uint> l_records;
		if(leftSmaller)
		{
			l_records = b.get_left_rel();
		}
		else
		{
			l_records = b.get_right_rel();
		}
		for(uint l_disk_id = 0; l_disk_id < l_records.size(); ++l_disk_id)
		{
			mem->loadFromDisk(disk, l_records.at(l_disk_id), input_mem_page);
			for(uint lRecordId = 0; lRecordId < input_page->size(); ++lRecordId)
			{
				Record current_record = input_page->get_record(lRecordId);
				uint bucket_hash = current_record.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
				Page* current_page = mem->mem_page(bucket_hash);

				current_page->loadRecord(current_record);
			}
			
			
		}	
		vector<uint> r_records;
		if(leftSmaller)
		{
			r_records = b.get_right_rel();
		}
		else
		{
			r_records = b.get_left_rel();
		}	
			
		for(uint r_disk_id = 0; r_disk_id < r_records.size(); ++r_disk_id)
		{
			input_page->reset();
			mem->loadFromDisk(disk, r_records.at(r_disk_id), input_mem_page);

			for(uint rRecordId = 0; rRecordId < input_page->size(); ++rRecordId)
			{
				Record current_record = input_page->get_record(rRecordId);
				uint bucket_hash = current_record.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
				Page* current_page = mem->mem_page(bucket_hash); // bucket h2 

				for(uint lRecordId = 0; lRecordId < current_page->size(); ++lRecordId)
				{	
					Record bucket_record = current_page->get_record(lRecordId);
					if (current_record == bucket_record)
					{
						output_page->loadPair(bucket_record, current_record);
						if(output_page->full())
						{
							uint out_disk_page = mem->flushToDisk(disk, output_mem_page);
							disk_pages.push_back(out_disk_page);
						}
						
					}
				}
			}		
		}
	}

	
	if(!output_page->empty())
	{
		uint out_disk_page = mem->flushToDisk(disk, output_mem_page);
		disk_pages.push_back(out_disk_page);
	}
	return disk_pages;
}
