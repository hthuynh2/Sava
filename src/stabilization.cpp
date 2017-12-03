#include "operations.h"

using replicas_t = std::set<int>;
using row_num_t = int;
using vm_id_t = int;


replicas_t route_file(std::string filename) {
    cout << "Inside route_file\n";
    std::hash<std::string> hash_fn;
    std::size_t first_replica = hash_fn(filename) % NUM_VMS;

    replicas_t ret;

    for(int i = 0, j = first_replica; i < NUM_REPLICAS; i++) {
        while(1){
            membership_list_lock.lock();
            if(membership_list.find(j) != membership_list.end() && ret.find(j) != ret.end()){   //j's in membership list
                membership_list_lock.unlock();
                break;
            }
            membership_list_lock.unlock();
            j = (j + 1) % NUM_VMS;
        }
        ret.insert(j);
        j++;
    }
    cout << ".....Inside route_file:" << " Done with route_file\n";
    return ret;
}

void handle_update(int& count, std::vector<file_update> & updates, std::mutex & updates_mutex, file_update tup, mutex &count_lock) {
    cout << "Inside handle_update\n";
    membership_list_lock.lock();
    VM_info M_y;
    bool M_y_fail_flag = false;
    if(membership_list.find(tup.M_y) == membership_list.end()){
         M_y.vm_num = -1;
         M_y_fail_flag =true;
    }
    else{
        M_y =  vm_info_map[tup.M_y];
    }
    membership_list_lock.unlock();

    if(M_y.vm_num != my_vm_info.vm_num || M_y_fail_flag == true){
        cout << "Inside handle_update: Tell " << M_y.vm_num << " to send to " << tup.M_x <<"\n";

        //Make connection with M_y

        int local_sock = -1;
        if(M_y_fail_flag == false){
            local_sock = tcp_open_connection(M_y.ip_addr_str, STABILIZATION_PORT);
        }

        //Create FTR msg and send to M_y
        string msg = create_FTR_msg(tup.filename, tup.version, tup.M_x);
        if(local_sock == -1 || M_y_fail_flag == true||tcp_send_string(local_sock, msg) == -1){     //Cannot send to M_y --> M_y failed
            // Get the node with the newest version of the file
            // that is not M_y
            cout << "M_y failed" <<"\n";
            int newest_ver = -1;
            row_num_t newest_ver_row;

            set<int> temp_set = filename_map[tup.filename];
            int sender = -1;
            membership_list_lock.lock();
            cout << "Finding new M_y\n";
            for(auto it = temp_set.begin(); it != temp_set.end(); it ++){
                int temp_rep = file_table[*it].replica;
                if(membership_list.find(temp_rep) != membership_list.end() ){
                    sender = temp_rep;
                    break;
                }
            }
            membership_list_lock.unlock();

            cout << "new M_y = " << sender<<"\n";


            // Schedule update
            updates_mutex.lock();
            file_update temp_fu(tup.filename, sender, tup.M_x, tup.version);
            cout << "choose new M_y = "  << sender <<"\n";
            cout << "Tell "<< sender << " send to " << tup.M_x <<"\n";
            updates.push_back(temp_fu);
            updates_mutex.unlock();
        }
        else{
            char buf[MAX_BUF_LEN];
            int numbytes = 0;
            struct timeval timeout_tv;
            timeout_tv.tv_sec = STAB_ACK_TIMEOUT;      //in sec
            timeout_tv.tv_usec = 0;
            setsockopt(local_sock, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
            string res;
            while(1){
                if((numbytes = recv(local_sock, buf, MAX_BUF_LEN, 0)) > 0){
                    string temp(buf,numbytes);
                    res = temp;
                    cout << "Inside handle_update: Get reply from M_y" << res<<"\n";
                    close(local_sock);
                    break;
                }
                membership_list_lock.lock();
                if(membership_list.find(tup.M_y) == membership_list.end()){
                    membership_list_lock.unlock();
                    // Get the node with the newest version of the file
                    // that is not M_y
                    cout << "Inside handle_update: M_y die, find another M_y\n";
                    int newest_ver = -1;
                    row_num_t newest_ver_row;

                    for(auto && k : filename_map[tup.filename]) {
                        if(file_table[k].version > newest_ver && file_table[k].replica != tup.M_y) {
                            newest_ver = file_table[k].version;
                            newest_ver_row = file_table[k].row;
                        }
                    }

                    // Get the node which has the file to send
                    vm_id_t sender = file_table[newest_ver_row].replica;

                    // Schedule update
                    updates_mutex.lock();

                    file_update temp_fu(tup.filename, sender, tup.M_x, tup.version);

                    updates.push_back(temp_fu);
                    updates_mutex.unlock();
                    close(local_sock);
                    break;
                }
                membership_list_lock.unlock();
            }
            if(res.size() >0 ){
                //DO nothing
            }
        }
    }
    else{
        cout << "Inside handle_update: I'm M_y\n";
        string msg = create_FTR_msg(tup.filename, tup.version, tup.M_x);
        cout << "Inside handle_update: create_FTR_msg:" << msg<< "\n";
        string is_done = handle_FTR_msg(-1,  msg, false);
        if(is_done == "1"){
            cout << "sent file to M_x\n";
        }
        else{
            cout << "Failed to send file to M_x\n";
        }
    }

    count_lock.lock();
    count = count -1;
    count_lock.unlock();
}


void node_fail_handler_at_master(vm_id_t node, int cur_master_id, int cur_master1_id, int cur_master2_id){
    cout << "Inside node_fail_handler_at_master: node failed :" <<  node << "\n";

    std::vector<file_update> updates;
    std::mutex updates_mutex;
    cout << "Inside node_fail_handler_at_master: ";
    if(replica_map.find(node) == replica_map.end()){
        cout << "No replica is store at this node\n";
        for(auto it = file_table.begin(); it != file_table.end() ;it++){
            cout << it->second.filename << " " <<it->second.replica << " ||||| ";
        }
    }
    cout <<"\n";

    // Replicate the keys on the failed node
    // Get all file rows with replica 'node'
    for(auto && i : replica_map[node]) {
        auto j = file_table[i];
        // Get the row with the newest version of 'j.filename'
        int newest_ver = -1;
        row_num_t newest_ver_row;
        for(auto && k : filename_map[j.filename]) {
            if(file_table[k].version > newest_ver) {
                newest_ver = file_table[k].version;
                newest_ver_row = file_table[k].row;
            }
        }

        // Get M_y
        vm_id_t M_y = file_table[newest_ver_row].replica;
        cout << "Inside node_fail_handler_at_master: M_y: " << M_y<<"\n";
        // Get M_x
        vm_id_t M_x;
        // replicas_t replicas = route_file(file_table[newest_ver_row].filename);      //NOTE: To avoid re-transmit file, M_x be the next alive node after M_y

/////
        set<int> replicas;
        std::hash<std::string> hash_fn;
        int first_replica = hash_fn(j.filename) % NUM_VMS;
        membership_list_lock.lock();
        cout << "asdasdasasd";
        cout << "Inside handle_WR_msg: get membershiplist_lock\n";
        // cout << "Inside handle_WR_msg: membership_list size = " << membership_list.size();
        int count = 3;
        if(membership_list.size() < 3){
            cout << "Inside node_fail_handler_at_master: membership_list size < 3\n";
            membership_list_lock.unlock();
            return ;
        }
        cout << "membership_list_size : " << membership_list.size();
        while(count>0){
            // cout << "asdsdasd\n";
            if(membership_list.find(first_replica) != membership_list.end() && replicas.find(first_replica) == replicas.end()){
                replicas.insert(first_replica);
                cout << "Inside node_fail_handler_at_master: add " <<first_replica << "into reps\n";
                count--;
            }
            first_replica = (first_replica+1)%100;
        }
        membership_list_lock.unlock();

        cout << "replicas = ";
        for(auto it = replicas.begin(); it != replicas.end(); it++){
            cout << *it << " ";
        }
        cout <<"\n";
//////
        std::set<row_num_t> file_stores = filename_map[file_table[newest_ver_row].filename];

        //assert(file_stores.size() == NUM_REPLICAS);
        for(auto && k : file_stores) {
            replicas.erase(file_table[k].replica);
        }

        M_x = *replicas.begin();

        cout << "Inside node_fail_handler_at_master: M_x = " << M_x<<"\n";
        file_update temp_fu(file_table[newest_ver_row].filename, M_y, M_x, newest_ver);
        updates.push_back(temp_fu);
    }

    if(updates.size() == 0){
        cout << "Inside node_fail_handler_at_master: Updates is empty\n";
    }
    else{
        cout << "Inside node_fail_handler_at_master: Start updating\n";
    }
    // Add all updates to file table
    for(auto && i : updates) {
        std::vector<int> intersection_result;
        std::set<row_num_t> & filename_set = filename_map[i.filename];
        std::set<row_num_t> & M_x_set = replica_map[i.M_x];

        std::set_intersection(
                              filename_set.begin(),
                              filename_set.end(),
                              M_x_set.begin(),
                              M_x_set.end(),
                              //                std::back_inserter(intersection_result.begin())
                              intersection_result.begin()
                              );

        if(intersection_result.empty()) {   //If row is not in the table, add it to the table
            cout << "Add row with M_x = " << i.M_x << " filename = " << i.filename << "to the updates\n";
            int temp_row = file_table.rbegin()->first + 1;
            while (file_table.find(temp_row) != file_table.end()) {
                temp_row ++;
            }
            file_row temp_f_row(temp_row, i.filename, i.M_x, i.version);
            file_table[temp_row] = temp_f_row;
            set<int> temp_rep = replica_map[i.M_x];
            temp_rep.insert(temp_row);
            replica_map[i.M_x] = temp_rep;

            set<int> file_rows = filename_map[i.filename];
            file_rows.insert(temp_row);
            filename_map[i.filename] = file_rows;
        }
        else if(intersection_result.size() >1){
            cout << "Having 2 replicas storing at same VM ???? Something is wrong! \n";
        }
        else{   //If row is already in the table update the version of row that has file and M_x
            cout << "File already in the table. Update the version to " <<i.version <<"\n";
            file_table[intersection_result[0]].version = i.version;
        }
    }
    // for(auto it = file_table.begin(); it != file_table.end(); it++){
    //     cout << it->second.filename << " " << it->second.replica <<"\n";
    // }

    // Atomic variable to count the number of running threads
    // std::atomic<int> count(0);
    int count = 0;
    mutex count_lock;

    // If new elements are added to 'updates' run new thread
    while(1){
        updates_mutex.lock();
        if(updates.empty() && count == 0){
            updates_mutex.unlock();
            break;
        }
        else if(!updates.empty()){
            membership_list_lock.lock();
            if(membership_list.find((*updates.begin()).M_x) == membership_list.end()){     /*tup.M_x failed*/
                membership_list_lock.unlock();
                updates.erase(updates.begin());
            }
            else{
                membership_list_lock.unlock();
                count_lock.lock();
                count++;
                count_lock.unlock();
                std::thread t(handle_update, ref(count), ref(updates), ref(updates_mutex), *updates.begin(), ref(count_lock));
                t.detach();
                updates.erase(updates.begin());
            }
        }
        updates_mutex.unlock();
    }

    cout << "Inside node_fail_handler_at_master: All updates are done\n";
    // Delete all the rows in the file_table that have 'node' as a replica
    //Delete filename_map
    for(auto && i : replica_map[node]) {
        set<int> file_rows = filename_map[file_table[i].filename];
        file_rows.erase(i);
        filename_map[file_table[i].filename] = file_rows;
    }
    //delete file_table
    for(auto && i : replica_map[node]) {
        file_table.erase(i);
    }

    // Delete the node from the replicas map
    replica_map.erase(node);
    for(auto it = file_table.begin(); it != file_table.end(); it++){
        cout << it->second.filename << " " << it->second.replica <<"\n";
    }


    int new_master2_id = -1;

    VM_info master1_info, master2_info, new_master2_info;

    membership_list_lock.lock();
    if(membership_list.find(cur_master1_id) != membership_list.end()){
        master1_info = vm_info_map[cur_master1_id];
    }
    else{
        master1_info.vm_num = -1;       //To mark that master 1 failed
    }

    if(membership_list.find(cur_master2_id) != membership_list.end()){
        master2_info = vm_info_map[cur_master2_id];
    }
    else{
        master2_info.vm_num = -1;       //To mark that master 1 failed
    }
    membership_list_lock.unlock();


    if(node == cur_master1_id || node == cur_master2_id){
        cout << "Inside node_fail_handler_at_master: failed node is master 1 or 2\n";
        //Choose randomly VM_k to be new master2 if the failed node is any of the 2 old masters
        membership_list_lock.lock();
        for(auto it = membership_list.begin(); it != membership_list.end(); it++){
            if(*it != cur_master_id && *it != cur_master1_id && *it != cur_master2_id){
                new_master2_id = *it;
                break;
            }
        }

        if(new_master2_id != -1){
            cout << "new _master2 is "<<new_master2_id<<"\n";
            new_master2_info = vm_info_map[new_master2_id];
        }
        else{
            cout << "There is less than 3 VMs in the system!!! This should never happen!!\n";
            exit(1);
        }
        membership_list_lock.unlock();
    }

    if(node == cur_master1_id){     //If node failed is master1
        string msg;
        if(master2_info.vm_num != -1){  //If old master 2 still alive
            //Tell old master2 to become new master 1 && tell that it will choose VM_k to be master2 && update its file table
            msg = create_MU_msg(node, cur_master2_id, new_master2_id);
            int master2_sock_fd = tcp_open_connection(master2_info.ip_addr_str, STABILIZATION_PORT);
            if(master2_sock_fd != -1){  //Can make connection with old_master2
                int numbytes = tcp_send_string(master2_sock_fd, msg);        //Don't care if send successfully or not!
                if(numbytes == 0)
                    close(master2_sock_fd);
            }
        }
        else{
            msg = create_MU_msg(node, 99, new_master2_id);        //use 99 to indicate that old master2 failed
        }
        cout << "MU_msg1: " << msg<<"\n";
        //Tell VM_k to become master 2 && Send the whole file table to new master 2
        int new_master2_sock_fd = tcp_open_connection(new_master2_info.ip_addr_str, STABILIZATION_PORT);
        if(new_master2_sock_fd != -1){                                  //Can make connection with new_master2
            int numbytes = tcp_send_string(new_master2_sock_fd, msg);        //Don't care if send successfully or not!
            if(numbytes == 0)
                close(new_master2_sock_fd);
        }
        cout << "Set master2 = "<< new_master2_id<<"\n";
        cout << "Set master1 = "<< master2_id<<"\n";
        master1_id = master2_id;
        master2_id = new_master2_id;
        // cout << "Set master1 = " <<
    }
    else if (node == cur_master2_id){
        string msg;
        if(master1_info.vm_num != -1){              //If old master 1 still alive
            //Tell old master2 to become new master 1 && tell that it will choose VM_k to be master2 && update its file table
            msg = create_MU_msg(node, cur_master1_id, new_master2_id);
            int master1_sock_fd = tcp_open_connection(master1_info.ip_addr_str, STABILIZATION_PORT);
            if(master1_sock_fd != -1){  //Can make connection with old_master2
                int numbytes = tcp_send_string(master1_sock_fd, msg);        //Don't care if send successfully or not!
                if(numbytes == 0)
                    close(master1_sock_fd);
            }
        }
        else{
            msg = create_MU_msg(node, 99, new_master2_id);        //use 99 to indicate that old master2 failed
        }
        cout << "MU_msg2: " << msg<<"\n";
        //Tell VM_k to become master 2 && Send the whole file table to new master 2
        int new_master2_sock_fd = tcp_open_connection(new_master2_info.ip_addr_str, STABILIZATION_PORT);
        if(new_master2_sock_fd != -1){                                  //Can make connection with new_master2
            int numbytes = tcp_send_string(new_master2_sock_fd, msg);        //Don't care if send successfully or not!
            if(numbytes == 0)
                close(new_master2_sock_fd);
        }
        cout << "Set master2 = "<< new_master2_id<<"\n";
        master2_id = new_master2_id;
    }
    else{           //Both master1 and master2 are still alive. Send updated file table to both of them
        string msg = create_MU_msg(node, cur_master1_id, cur_master2_id);

        cout << "Inside node_fail_handler_at_master:: MU_msg " << msg <<"\n";
        cout << "Inside node_fail_handler_at_master:: master1, master2: "<<cur_master1_id << " "<< cur_master2_id<<"\n";
        int master1_sock_fd = tcp_open_connection(master1_info.ip_addr_str, STABILIZATION_PORT);
        if(master1_sock_fd != -1){                                      //Can make connection with new_master1
            cout << "Inside node_fail_handler_at_master: send MU msg to master1\n";
            int numbytes = tcp_send_string(master1_sock_fd, msg);        //Don't care if send successfully or not!
            if(numbytes == 0)
                close(master1_sock_fd);
        }

        int master2_sock_fd = tcp_open_connection(master2_info.ip_addr_str, STABILIZATION_PORT);
        if(master2_sock_fd != -1){                                      //Can make connection with new_master2
            cout << "Inside node_fail_handler_at_master: send MU msg to master2\n";
            int numbytes = tcp_send_string(master2_sock_fd, msg);        //Don't care if send successfully or not!
            if(numbytes == 0)
                close(master2_sock_fd);
        }
    }
}

void node_fail_handler_at_master1(vm_id_t node){
    cout << "Inside node_fail_handler_at_master1\n";
    if(node == master_id){          //I'll become the master!
    cout << "Inside node_fail_handler_at_master1 : Node failed is master.\n";
        if(last_failed_node != -1){ //If there is another node failed before the master fail
            //Choose another node to be new master 1
            cout << "Inside node_fail_handler_at_master1 : There is one node failed before master.\n";

            if(last_failed_node == master2_id){         //Last failed node is master2
                cout << "Inside node_fail_handler_at_master1 : last failed is master2.\n";

                //Choose two new master1 and master2
                int new_master1_id = -1;
                int new_master2_id = -1;
                membership_list_lock.lock();
                for(auto it = membership_list.begin(); it != membership_list.end(); it++){
                    if(*it != my_vm_info.vm_num && *it != node && *it != last_failed_node){
                        if(new_master1_id == -1){
                            new_master1_id = *it;
                        }
                        else if(new_master2_id == -1){
                            new_master2_id = *it;
                        }
                        else{
                            break;
                        }
                    }
                }
                membership_list_lock.unlock();
                cout << "Inside node_fail_handler_at_master1 : new_master1, new_master2 = " << new_master1_id << " "<<new_master2_id << "\n";

                //Handle failure at last failed node
                node_fail_handler_at_master(last_failed_node, my_vm_info.vm_num, new_master1_id, new_master2_id);

                //Hanlde failure at current failed node
                node_fail_handler_at_master(node, my_vm_info.vm_num, new_master1_id, new_master2_id);

                //Already handle 2 failure. Reset last_failed_node
                last_failed_node = -1;

                master1_id = new_master1_id;
                master2_id = new_master2_id;
            }
            else{                   //Last failed node is normal node
                //Choose a new node to be new master 1
                cout << "Inside node_fail_handler_at_master1 : last node fail is normal node.\n";
                int new_master2_id = -1;
                membership_list_lock.lock();
                for(auto it = membership_list.begin(); it != membership_list.end(); it++){
                    if(*it != my_vm_info.vm_num && *it != node && *it != master2_id){
                        if(new_master2_id == -1){
                            new_master2_id = *it;
                            break;
                        }
                    }
                }
                membership_list_lock.unlock();

                //Handle failure at last failed node
                node_fail_handler_at_master(last_failed_node, my_vm_info.vm_num, master2_id, new_master2_id);
                //Hanlde failure at current failed node
                node_fail_handler_at_master(node, my_vm_info.vm_num, master2_id, new_master2_id);
                //Already handle 2 failure. Reset last_failed_node
                last_failed_node = -1;

                //master1 will become new master;
                //master2 will beomce new master1
                master1_id = master2_id;
                master2_id = new_master2_id;
            }
        }
        else{                       //There is no node failed before this
            //Choose a new node to be new master 2
            cout << "Inside node_fail_handler_at_master1 : There is no node failed before master.\n";
            int new_master2_id = -1;
            membership_list_lock.lock();
            for(auto it = membership_list.begin(); it != membership_list.end(); it++){
                if(*it != my_vm_info.vm_num && *it != node && *it != master2_id){
                    if(new_master2_id == -1){
                        new_master2_id = *it;
                        break;
                    }
                }
            }
            membership_list_lock.unlock();
            cout << "Inside node_fail_handler_at_master1 : new master2_id = " << new_master2_id<<"\n";

            //Hanlde failure at current failed node
            node_fail_handler_at_master(node, my_vm_info.vm_num, master2_id, new_master2_id);
            //Already handle failure. Reset last_failed_node
            last_failed_node = -1;

            //master1 will become new master;
            //master2 will beomce new master1
            //Need to be in this order to prevent system from hanging
            master1_id = master2_id;
            master2_id = new_master2_id;
        }

        //Update myself as master!
        master_id = my_vm_info.vm_num;
        cout << "Inside node_fail_handler_at_master1 : Update itself to master\n";

        //Send out msg to say that I'm master
        string msg = create_M_msg(master_id);
        vector<string> ip_strings;
        membership_list_lock.lock();
        for(auto it = membership_list.begin(); it != membership_list.end(); it++){
            if(*it != my_vm_info.vm_num)
                ip_strings.push_back(vm_info_map[*it].ip_addr_str);
        }
        membership_list_lock.unlock();

        //Send all msg to other VMs. Is there a better way to do this???? This seems too slow
        for(int i = 0; i < (int) ip_strings.size(); i++){
            int temp_sock_fd = tcp_open_connection(ip_strings[i], STABILIZATION_PORT);
            if(temp_sock_fd != -1){
                int numbytes = tcp_send_string(temp_sock_fd, msg);
                if(numbytes == 0 )
                    close(temp_sock_fd);
            }
        }
        cout << "Send msg to other vm to tell that it is master\n";
    }
    else{
        cout << "Inside node_fail_handler_at_master1 : Node failed is not master. Do nothing\n";
        //Mark this one as last_failed_node and wait for Master to handle it!!
        last_failed_node = node;
    }
}


void node_fail_handler_at_master2(vm_id_t node){
    cout << "Inside node_fail_handler_at_master2\n";
    if((node == master_id && last_failed_node == master1_id) ||((node == master1_id) && (last_failed_node == master_id))) {          //I'll become the master!
        //Choose two new master1 and master2
        cout << "Inside node_fail_handler_at_master2: Both master 1 and master failed\n";

        int new_master1_id = -1;
        int new_master2_id = -1;
        membership_list_lock.lock();
        for(auto it = membership_list.begin(); it != membership_list.end(); it++){
            if(*it != my_vm_info.vm_num && *it != node && *it != last_failed_node){
                if(new_master1_id == -1){
                    new_master1_id = *it;
                }
                else if(new_master2_id == -1){
                    new_master2_id = *it;
                }
                else{
                    break;
                }
            }
        }
        membership_list_lock.unlock();
        cout << "Inside node_fail_handler_at_master2: new_master1, new_master2_id " << new_master1_id <<" "<< new_master2_id<<"\n";

        //Handle failure at last failed node
        node_fail_handler_at_master(last_failed_node, my_vm_info.vm_num, new_master1_id, new_master2_id);

        //Hanlde failure at current failed node
        node_fail_handler_at_master(node, my_vm_info.vm_num, new_master1_id, new_master2_id);

        //Already handle 2 failure. Reset last_failed_node
        last_failed_node = -1;

        //Send out msg to say that I'm master
        //Update myself as master!
        master_id = my_vm_info.vm_num;

        //Update master1 and master2
        master1_id = new_master1_id;
        master2_id = new_master1_id;

        //Send out msg to say that I'm master
        string msg = create_M_msg(master_id);
        vector<string> ip_strings;
        membership_list_lock.lock();
        for(auto it = membership_list.begin(); it != membership_list.end(); it++){
            if(*it != my_vm_info.vm_num)
                ip_strings.push_back(vm_info_map[*it].ip_addr_str);
        }
        membership_list_lock.unlock();
        //Send all msg to other VMs. Is there a better way to do this???? This seems too slow
        for(int i = 0; i < (int)ip_strings.size(); i++){
            int temp_sock_fd = tcp_open_connection(ip_strings[i], STABILIZATION_PORT);
            if(temp_sock_fd != -1){
                int numbytes = tcp_send_string(temp_sock_fd, msg);
                if(numbytes == 0 )
                    close(temp_sock_fd);
            }
        }
        cout << "Inside node_fail_handler_at_master2: send out msg say that it is master";
    }
    else{
        //Mark this as last_failed node and wait for Master and Master1 to handle it!!
        cout << "Inside node_fail_handler_at_master2: master & master 1 still alive ";
        last_failed_node = node;
    }
}


// Called whenever a node is detected to fail
void node_fail_handler(vm_id_t node) {
    file_table_lock.lock();
    cout << "Inside node_fail_handler failed node = " << node <<"\n";
    if(my_vm_info.vm_num == master_id) {        //I'm the Master
        cout << "I am master\n";
        node_fail_handler_at_master(node, my_vm_info.vm_num, master1_id, master2_id);
    }
    else {          //
        if(my_vm_info.vm_num == master1_id){
            cout << "I'm master1\n";
            node_fail_handler_at_master1(node);
        }
        else if(my_vm_info.vm_num == master2_id){
            cout << "I'm master2\n";
            node_fail_handler_at_master2(node);
        }
        else{
            cout << "Inside node_fail_handler: Not master or master1,2 DO nothing\n";
        }
    }
    file_table_lock.unlock();
    // //Check if there is any node waiting to hanled. If yes, handle it!
    // waiting_to_handle_fail_id_lock.lock();
    // if(waiting_to_handle_fail_id != -1){
    //     int node_to_handle = waiting_to_handle_fail_id;
    //     waiting_to_handle_fail_id = -1;
    //     waiting_to_handle_fail_id_lock.unlock();
    //     node_fail_handler(node_to_handle);
    // }
}


//////////////////////////////////////
//File Transfer Request msg
string create_FTR_msg(string file_name, int version, int M_x){
    string msg("FTR");
    msg += int_to_string(version);
    msg += int_to_string(M_x);
    msg += file_name;
    return msg;
}

string create_MU_msg(int failed_node, int master1_id, int master2_id){
    string msg("MU");
    msg += int_to_string(failed_node);
    if(master1_id < 0){
        master1_id = 99;
    }
    if(master2_id < 0 ){
        master2_id = 99;
    }
    msg += int_to_string(master1_id);
    msg += int_to_string(master2_id);
    msg += "\n";
    if(failed_node == 99){
        return msg;
    }
    
    if(file_table.size() == 0){
        cout << "Inside create_MU_msg: file table is empty\n ";
    }
    next_version_map_lock.lock();
    for(auto it = file_table.begin(); it != file_table.end(); it++){
        string file_str("");
        file_str += int_to_string(it->second.replica);
        file_str += int_to_string(it->second.version);
        file_str += int_to_string(next_version_map[it->second.filename]);
        file_str += it->second.filename;
        file_str += "\n";
        msg += file_str;
    }
    next_version_map_lock.unlock();
    return msg;
}


string create_M_msg(int master_id){
    string msg("M");
    msg += int_to_string(master_id);
    return msg;
}


string handle_FTR_msg(int socket_fd, string msg, bool need_to_send){
    cout << "Inside handle_FTR_msg\n";

    // const char* pch = strchr(msg.c_str(), '\n');
    // int file_name_length = pch - msg.c_str() - 4;
    // string file_name = msg.substr(3, file_name_length);
    // int version = string_to_int(msg.substr(pch-msg.c_str()+1, 2));
    // int M_x = string_to_int(msg.substr(pch-msg.c_str()+1 +2, 2));


    int version = string_to_int(msg.substr(3,2));
    int M_x = string_to_int(msg.substr(5,2));
    string file_name = msg.substr(7);
    VM_info M_x_info;
    membership_list_lock.lock();

    cout << "Inside handle_FTR_msg: file_name = " << file_name << "\n";
    cout << "M_x = "<< M_x<<"\n";

    if(membership_list.find(M_x) != membership_list.end()){
        M_x_info = vm_info_map[M_x];
        membership_list_lock.unlock();
    }
    else{
        membership_list_lock.unlock();
        return "0";
    }

    if(check_and_write_file(file_name, file_name, M_x_info.ip_addr_str, STABILIZATION_PORT, version) == true){
        string yes_str("1");
        if(need_to_send){
            send(socket_fd, yes_str.c_str(), yes_str.size(), 0);
        }
        else{
            return yes_str;
        }
    }
    else{
        string no_str("0");
        if(need_to_send){
            send(socket_fd, no_str.c_str(), no_str.size(), 0);
        }
        else{
            return no_str;
        }
    }
    return "0";
}

void handle_MU_msg(string msg){
    cout << "Inside handle_MU_msg " << msg <<"\n";
    file_table_lock.lock();
    vector<string> lines;

    string delimiter = "\n";
    size_t pos = 0;
    std::string token;
    while ((pos = msg.find(delimiter)) != std::string::npos){
        token = msg.substr(0, pos);
        token.push_back('\n');
        lines.push_back(token);
        msg.erase(0, pos + delimiter.length());
    }

    int new_master1 , new_master2, node_fail_id;
    node_fail_id = string_to_int(lines[0].substr(2, 2));
    // new_master = string_to_int(lines[0].substr(4,2));
    new_master1 = string_to_int(lines[0].substr(4, 2));
    new_master2 = string_to_int(lines[0].substr(6, 2));
    cout << "Inside handle MU_msg: lines size = " << lines.size() <<"\n";
    master_lock.lock();
    // master_id = new_master;
    master1_id = new_master1;
    master2_id = new_master2;
    master_lock.unlock();
    cout << "Inside handle_MU_msg: Update master1/2: "<< new_master1<<" "<< new_master2<<"\n";
    
    //
    if(node_fail_id == 99 ){        //Msg is used only to update master!
        file_table_lock.unlock();
        return;
    }
        
        
    file_table.erase(file_table.begin(), file_table.end());
    next_version_map_lock.lock();
    next_version_map.erase(next_version_map.begin(), next_version_map.end());
    replica_map.erase(replica_map.begin(), replica_map.end());
    filename_map.erase(filename_map.begin(), filename_map.end());

    for(int i = 1; i < (int)lines.size(); i++){
        int replica_id = string_to_int(lines[i].substr(0,2));
        int version = string_to_int(lines[i].substr(2,2));
        int next_version = string_to_int(lines[i].substr(4,2));
        string file_name = lines[i].substr(6, lines[i].size()- 7);

        cout << file_name << " "<< replica_id << " "<< version << " "<<next_version << "\n";
        // const char* pch = strchr(lines[i].c_str(), '?');
        // int file_name_length = pch - msg.c_str();
        // string file_name = msg.substr(0, file_name_length);
        // int replica_id = string_to_int(msg.substr(file_name_length+1, 2));
        // int version = string_to_int(msg.substr(file_name_length+1+2, 2));
        // int next_version = string_to_int(msg.substr(file_name_length+1+2+2, 2));

        file_row new_row;
        // new_row.row = i;
        // new_row.file_name = file_name;
        // new_row.version = version;
        // new_row.replica = replica_id;

        new_row.row = i;
        new_row.filename = file_name;
        new_row.replica = replica_id;
        new_row.version = version;

        file_table[i] = new_row;
        next_version_map[file_name] = next_version;
        replica_map[replica_id].insert(i);
        filename_map[file_name].insert(i);
    }

    if(last_failed_node == node_fail_id){
        last_failed_node = -1;
    }
    if(waiting_to_handle_fail_id == node_fail_id){
        waiting_to_handle_fail_id = -1;
    }

    cout << "Inside handle_MU_msg: file_table: \n";

    for(auto it = file_table.begin(); it != file_table.end();it++){
        cout << it->first << "   "<< it->second.filename << " "<< it->second.replica << "\n";
    }

    cout << "Inside handle_MU_msg: filenam_map: \n";
    for(auto it = filename_map.begin(); it != filename_map.end();it++){
        set<int> temp_s = it->second;
        cout << it->first << " | ";
        for(auto i = temp_s.begin(); i != temp_s.end(); i++){
            cout << *i << " ";
        }
        cout <<"\n";
    }
    cout << "Inside handle_MU_msg: rep_map: \n";


    for(auto it = replica_map.begin(); it != replica_map.end();it++){
        set<int> temp_s = it->second;
        cout << it->first << " | ";
        for(auto i = temp_s.begin(); i != temp_s.end(); i++){
            cout << *i << " ";
        }
        cout <<"\n";
    }


    next_version_map_lock.unlock();
    file_table_lock.unlock();

    cout << "Inside handle_MU_msg: Done\n";

    return;
}

void handle_M_msg(string msg){
    master_lock.lock();
    int new_master = string_to_int(msg.substr(1,2));
    master_id = new_master;
    master_lock.unlock();
}
