#ifndef common1_h
#define common1_h

extern mutex is_computing_lock;
extern bool is_computing;
void start_application(string& application_name, map<int,int>& worker_map, int& master_vm_num);




extern int64_t total_receive;
extern int64_t total_send;



#endif












