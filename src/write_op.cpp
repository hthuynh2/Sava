#include "operations.h"
#include <sys/stat.h>


string create_WR_msg(string file_name){
    string msg("WR");
    msg += file_name;
    return msg;
}


string create_WT_msg(int rep1, int rep2, int rep3, int version, bool flag){
    string msg("WT");
    msg += int_to_string(rep1);
    msg += int_to_string(rep2);
    msg += int_to_string(rep3);
    msg += int_to_string(version);
    if(flag == false){
        msg += "0";
    }
    else{
        msg += "1";
    }
    return msg;
}

string create_WA_msg(int rep1,int  rep2,int rep3, int version, string file_name){
    string msg("WA");
    msg += int_to_string(rep1);
    msg += int_to_string(rep2);
    msg += int_to_string(rep3);
    msg += int_to_string(version);
    msg += file_name;
    return msg;
}

void thread_check_and_send(string file_name, string sdfs_file_name, string ip_addr_str, string port, int version, int& result){
    bool temp =  check_and_write_file(file_name, sdfs_file_name, ip_addr_str, port, version);
    if(temp == true){
        result = 0;
    }
    else{
        result = 1;
    }
}

bool write_at_client(string file_name, string sdfs_file_name){
    cout << "Inside write_at_client\n";
    timepnt begin = clk::now();

    FILE* temp;
    if((temp = fopen(file_name.c_str(), "r"))== NULL){
        cout << "Cannot open file with name: "<< file_name;
        return false;
    }

    bool isMaster = false;
    master_lock.lock();
    int cur_master_id = master_id;
    if(master_id == my_vm_info.vm_num){
        isMaster = true;
    }
    master_lock.unlock();

    string wt_msg("");
    string wr_msg = create_WR_msg(sdfs_file_name);
    VM_info master_info;
    if(isMaster == false){
        membership_list_lock.lock();
        if(membership_list.find(cur_master_id) == membership_list.end()){
            membership_list_lock.unlock();
            return false;
        }
        master_info =  vm_info_map[cur_master_id];
        membership_list_lock.unlock();

        //Send write request msg to S
        int master_sock_fd = tcp_open_connection(master_info.ip_addr_str, MASTER_PORT);
        if(master_sock_fd == -1)
            return false;

        //Create Write Request msg

        int numbytes = tcp_send_string(master_sock_fd, wr_msg);
        if(numbytes != 0)
            return false;

        struct timeval timeout_tv;
        timeout_tv.tv_sec = WRITE_RQ_TIMEOUT;      //in sec
        timeout_tv.tv_usec = 0;
        setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

        char buf[MAX_BUF_LEN];
        numbytes = recv(master_sock_fd, buf, MAX_BUF_LEN,0);
        if(numbytes <= 0){      //Time out
            close(master_sock_fd);
            return false;
        }
        close(master_sock_fd);
        string temp(buf, numbytes);
        if(temp.size() != 11){
            cout << "WT msg does not have size of 10. Something is WRONG!!\n";
            return false;
        }
        else{
            wt_msg = temp;
        }
    }
    else{
        wt_msg =  handle_WR_msg(-1, wr_msg, false);
        if(wt_msg.size() != 11){
            cout << "WT msg does not have size of 11. Something is WRONG!!\n";
            return false;
        }
        else{
            cout << "Inside write_at_client: WT msg= " << wt_msg << "\n";
        }
    }

    int rep1 = string_to_int(wt_msg.substr(2,2));
    int rep2 = string_to_int(wt_msg.substr(4,2));
    int rep3 = string_to_int(wt_msg.substr(6,2));
    int version = string_to_int(wt_msg.substr(8,2));
    char flag_char = wt_msg[10];
    cout << "Inside write_at_client: "  << wt_msg<<"\n";
    cout << "Inside write_at_client: " << rep1 <<" "<<rep2 << " " << rep3<<"\n";
    VM_info rep1_info, rep2_info, rep3_info;
    cout << "flag_char == " << flag_char <<"\n";
    if(flag_char == '1'){   //There is a write in less than 30 secs
        timepnt now = clk::now();
        cout << std::chrono::duration_cast<unit_milliseconds>(now-begin).count() <<"\n";

        cout << "There is write happened in last 60secs. Press yes to continue. Press no to stop\n";
        fd_set r_master, r_fds;
        FD_ZERO(&r_master);
        FD_ZERO(&r_fds);
        FD_SET(0, &r_master);
        char buf[MAX_BUF_LEN];

        r_fds = r_master;
        struct timeval t_out;
        t_out.tv_sec = 30;
        t_out.tv_usec = 0;

        if(select(0+1, &r_fds, NULL, NULL, &t_out) == -1){
            perror("input: select");
            exit(4);
        }
        if(FD_ISSET(0, &r_fds)){
            int num_read =  read(0, buf, MAX_BUF_LEN);
            string temp_str(buf, num_read);
            cout << temp_str<<"\n";
            if(strncmp(temp_str.c_str(), "yes",3) != 0){
                // if(strcmp(temp_str.c_str(), "yes", 3) != "yes"){
//
                cout << "Write operation is rejected. Please try again!\n";
                return false;
            }
        }
        else{
            cout << "Timeout. Write operation is rejected. Please try again!\n";
            return false;
        }
    }

    membership_list_lock.lock();
    if(membership_list.find(rep1) == membership_list.end()){
        cout << "Cannot find replica in membership list. Something is wrong "<< rep1<<"\n";
        membership_list_lock.unlock();
        return false;
    }
    else{
        rep1_info = vm_info_map[rep1];
    }
    if(membership_list.find(rep2) == membership_list.end()){
        cout << "Cannot find replica in membership list. Something is wrong "<< rep2<<"\n";
        membership_list_lock.unlock();
        return false;
    }
    else{
        rep2_info = vm_info_map[rep2];
    }

    if(membership_list.find(rep3) == membership_list.end()){
        cout << "Cannot find replica in membership list. Something is wrong "<< rep3<<"\n";
        membership_list_lock.unlock();
        return false;
    }
    else{
        rep3_info = vm_info_map[rep3];
    }
    membership_list_lock.unlock();

    // Can do these in parallel to make it faster!!! Use 3 threads to do it
    bool result = true;
    bool flag = false;

    //NOTE: Write only succeed when all 3 replicas alive during the writing time.
    //Can change this by re-send request to Master to get new replica

    int result1, result2,result3;
    result1 = result2 = result3 = 1;

    thread tid1, tid2, tid3;
    bool tid_flag1 = false;
    bool tid_flag2 = false;
    bool tid_flag3 = false;

    //NEED to check if replica is current vm
    if(rep1_info.vm_num != my_vm_info.vm_num){
        cout << "Start check_and_write_file with rep1 = " << rep1_info.vm_num<<"\n";
        tid_flag1 = true;
        tid1 = thread(thread_check_and_send, file_name, sdfs_file_name, rep1_info.ip_addr_str, OP_PORT, version, ref(result1));
    }
    else{
        result1 = 0;
        flag = true;
    }

    // thread t_id1, t_id2, t_id3;
    if(rep2_info.vm_num != my_vm_info.vm_num){
        cout << "Start check_and_write_file with rep2 = " << rep2_info.vm_num<<"\n";
        tid_flag2 = true;
        tid2 = thread(thread_check_and_send, file_name, sdfs_file_name, rep2_info.ip_addr_str, OP_PORT, version, ref(result2));
    }
    else{
        result2 = 0;
        flag = true;
    }

    if(rep3_info.vm_num != my_vm_info.vm_num){
        cout << "Start check_and_write_file with rep3 = " << rep3_info.vm_num<<"\n";
        tid_flag3 = true;
        tid3 = thread(thread_check_and_send, file_name, sdfs_file_name, rep3_info.ip_addr_str, OP_PORT, version, ref(result3));
    }
    else{
        result3 = 0;
        flag = true;
    }

    if(tid_flag1 == true){
        tid1.join();
    }
    if(tid_flag2 == true){
        tid2.join();
    }
    if(tid_flag3 == true){
        tid3.join();
    }
    if(result1 == 1 || result2 == 1 || result3 == 1){
        result = false;
    }

    cout << result1 << " " << result2 << " " << result3<<"\n";
    if(result == false){
        cout << "Inside write_at_client: result == false\n";
        return false;
    }

    if(flag == true){
        delivered_file_map_lock.lock();
        if(delivered_file_map.find(sdfs_file_name) != delivered_file_map.end()
            && delivered_file_map[sdfs_file_name].version >= version){
                cout << "Inside write_at_client: current deliver_file has newer version\n";
                delivered_file_map_lock.unlock();
        }
        else{
            delivered_file_map_lock.unlock();
            buffer_file_map_lock.lock();
            cout << "Inside write_at_client: copy to buffer\n";

            file_struct f_str;
            f_str.file_name = sdfs_file_name;
            f_str.version = version;
            string f_buffer_name(sdfs_file_name);
            f_buffer_name += int_to_string(version);
            buffer_file_map[f_buffer_name] = f_str;
            std::ifstream ifs(file_name, std::ios::binary); //Copy file
            std::ofstream ofs(f_buffer_name, std::ios::binary);
            ofs << ifs.rdbuf();
            buffer_file_map_lock.unlock();
        }
    }

    //NOTE: This send to old master. If the master fail during the write, then this is unsuccessful write
    //Can Change this by getting current master instead of using old master --> This is more complex.

    string wa_msg = create_WA_msg( rep1,  rep2, rep3, version, sdfs_file_name);
    if(isMaster == false){
        //Send msg to server say that finish writing
        int master_sock_fd = tcp_open_connection(master_info.ip_addr_str, MASTER_PORT);
        if(master_sock_fd == -1){
            cout << "fail to make connection with master.\n";
            return false;
        }

        //Create Write Ack msg
        if(tcp_send_string(master_sock_fd, wa_msg) == -1){
            return false;
        }
        struct timeval timeout_tv;
        timeout_tv.tv_sec = WRITE_RQ_TIMEOUT;      //in sec
        timeout_tv.tv_usec = 0;
        setsockopt(master_sock_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

        char buf[MAX_BUF_LEN];
        //Master reply with WS message
        int numbytes = recv(master_sock_fd, buf, MAX_BUF_LEN,0);
        if(numbytes <= 2){      //Time out
            close(master_sock_fd);
            return false;
        }
        if(buf[2] == '1'){      //If write succeed.
            close(master_sock_fd);
            cout << "Write successfully.\n";
            return true;
        }
        else{           //If write failed
            cout << "Write failed.\n";
            close(master_sock_fd);
            return false;
        }
    }
    else{
        string ws_msg = handle_WA_msg(-1, wa_msg, false);
        if(ws_msg.size() == 3 && ws_msg[2] == '1'){
            cout << "Write successfully. "<< ws_msg <<"\n";
            return true;
        }
        else{
            cout << "Write failed. "<< ws_msg<< "\n";
            return false;
        }
    }
}




string create_FC_msg(string file_name, int version){
    string msg("FC");
    msg += int_to_string(version);
    msg += file_name;
    return msg;
}

string create_FCR_msg(bool is_accept){
    string msg("FCR");
    if(is_accept)
        msg += "1";
    else
        msg += "0";
    return msg;
}

bool check_and_write_file(string file_name, string sdfs_file_name, string dest_ip, string dest_port, int version){
    cout << "inside check_and_write_file with name = "<< file_name <<"\n ";
    FILE* fp = fopen(file_name.c_str(), "r");

    if(fp == NULL){
        cout<< "Cannot open file\n";
        return false;                //Should I return true or false??
    }

    int socket_fd = tcp_open_connection(dest_ip, dest_port);
    if(socket_fd == -1)
        return false;

    string fc_msg = create_FC_msg(sdfs_file_name, version);
    int numbytes;
    if((numbytes = send(socket_fd, fc_msg.c_str(), fc_msg.size(), 0)) == -1){
        cout <<"Inside check_and_write_file: Fail to send FC_msg\n";
        close(socket_fd);
        return false;
    }
    else{
        cout <<"Inside check_and_write_file: successfully send FC_msg\n";
    }

    struct timeval timeout_tv;
    timeout_tv.tv_sec = WRITE_FC_TIMEOUT;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));


    char buf[MAX_BUF_LEN];
    numbytes = recv(socket_fd, buf, MAX_BUF_LEN,0);
    if(numbytes <= 3){      //Time out
        cout << "Inside check_and_write_file: Timeout!\n";
        close(socket_fd);
        return false;
    }

    char reply = buf[3];
    string temp_str(buf,3);
    cout <<reply;
    if(reply != '1'){       //Dont need to send file.
        cout << "Inside check_and_write_file: Don't need to send!\n";
        close(socket_fd);
        return true;
    }


    timeout_tv.tv_sec = 0;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

    int file_size;
    // fseek(fp, 0L, SEEK_END);
    // file_size = ftell(fp);
    // fseek(fp, 0L, SEEK_SET);


    int z; /* Status code */
    int s; /* Socket s */
    struct linger so_linger;
    so_linger.l_onoff = 1;
    so_linger.l_linger = 30;
    z = setsockopt(socket_fd, SOL_SOCKET, SO_LINGER, &so_linger, sizeof so_linger);
    if (z) {
       perror("setsockopt(2)");
       return false;
    }

    struct stat filestatus;
    stat(file_name.c_str(), &filestatus);
    file_size = filestatus.st_size ;

    string file_size_str = to_string(file_size);
    file_size_str += "\n";
    cout << "Inside check_and_write_file: Start sending file with size "<< file_size<< "!\n";
    cout << "Inside check_and_write_file: send to "<< dest_ip << "\n";

    //Send size of file
    if (send(socket_fd, file_size_str.c_str(), file_size_str.size(), 0) == -1){
        perror("send");
        fclose(fp);
        close(socket_fd);
        return false;
    }

    //Read from file and send data
    // int numbytes;
    int count = 0;
    while(count < file_size){
        numbytes = fread(buf, 1, MAX_BUF_LEN, fp);
        int sent_bytes = send(socket_fd, buf, numbytes, 0);
        if(sent_bytes != numbytes){
            cout << "Inside check_and_write_file: sent != numbytes\n";
        }
        count += numbytes;
    }

    // while((numbytes = fread(buf, 1, MAX_BUF_LEN, fp)) != 0){
    //     if (send(socket_fd, buf, numbytes, 0) == -1){
    //         perror("send");
    //         fclose(fp);
    //         close(socket_fd);
    //         return false;
    //     }
    //     count += numbytes;
    // }
    cout << "Sent "<< count << "bytes\n";

    fclose(fp);

    timeout_tv.tv_sec = 10;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
    if(recv(socket_fd, buf, MAX_BUF_LEN, 0) == -1){
        cout << "Inside check_and_write_file: NO receive ack!\n";
        return false;
    }
    else{
        cout << "Inside check_and_write_file: Done!\n";
        return true;
    }
    // close(socket_fd);

    // cout << "Inside check_and_write_file: Done!\n";
    return true;
}

string handle_WR_msg(int socket_fd, string msg, bool need_to_send){
    cout << "Inside handle_WR_msg...\n";
    string file_name = msg.substr(2);
    int rep1 = -1, rep2 = -1, rep3 = -1;
    int version ;
    file_table_lock.lock();
    cout << "Inside handle_WR_msg: Get file_table_lock\n";
    if(filename_map.find(file_name) == filename_map.end()){//file does not exist
        set<int> reps;
        std::hash<std::string> hash_fn;
        int first_replica = hash_fn(file_name) % NUM_VMS;
        membership_list_lock.lock();
        cout << "asdasdasasd";
        cout << "Inside handle_WR_msg: get membershiplist_lock\n";
        // cout << "Inside handle_WR_msg: membership_list size = " << membership_list.size();
        int count = 3;

        if(membership_list.size() < 3){
            cout << "Inside handle_WR_msg: membership_list size < 3\n";
            membership_list_lock.unlock();
            file_table_lock.unlock();
            return "";
        }
        while(count >0){
            cout << "asdsdasd\n";
            if(membership_list.find(first_replica) != membership_list.end()){
                reps.insert(first_replica);
                cout << "Inside handle_WR_msg: add " <<first_replica << "into reps\n";
                count--;
            }
            first_replica = (first_replica+1)%membership_list.size();
        }
        membership_list_lock.unlock();
        cout << "Inside handle_WR_msg: Get all reps\n";
        for(auto it = reps.begin(); it != reps.end(); it ++){
            if(rep1 == -1){
                rep1 = *it;
            }
            else if(rep2 == -1){
                rep2 = *it;
            }
            else if(rep3 == -1){
                rep3 = *it;
            }
            else
                break;
        }
        next_version_map_lock.lock();
        if(next_version_map.find(file_name) == next_version_map.end()){
            version = 1;
            next_version_map[file_name] = 2;
        }
        else{
            version = next_version_map[file_name];
            next_version_map[file_name]++;
        }
        next_version_map_lock.unlock();
        cout << "....Inside handle_WR_msg: " <<rep1 << " " << rep2 <<" "<<rep3 << "\n";
    }
    else{
        set<int> rows = filename_map[file_name];
        if(rows.size() != 3){    //Something is wrong!!
            cout << "File has less than 3 replica. Something is WRONG\n";
        }
        for(auto it = rows.begin(); it != rows.end(); it++){
            if(rep1 == -1){
                rep1 = file_table[*it].replica;
            }
            else if(rep2 == -1){
                rep2 = file_table[*it].replica;
            }
            else if(rep3 == -1){
                rep3 = file_table[*it].replica;
            }
            else
                break;
        }
        next_version_map_lock.lock();
        version = next_version_map[file_name];
        next_version_map[file_name] ++;
        next_version_map_lock.unlock();
    }
    file_table_lock.unlock();
    struct timeval tv;
    gettimeofday(&tv, NULL);
    bool flag = false;
    if(last_write_map.find(file_name) != last_write_map.end()){
        if(tv.tv_sec - last_write_map[file_name] < TIMECONFLICT){
            cout << "There is a write conflict\n";
            flag = true;
        }
    }
    last_write_map[file_name] = tv.tv_sec;
    if(flag == true){
        cout << "There is a conflict\n";
    }
    else{
        cout << "There is no conflict\n";
    }
    string wt_msg = create_WT_msg(rep1, rep2, rep3, version, flag);
    cout << "....Inside handle_WR_msg: " << wt_msg << "\n";
    if(need_to_send == true){
        tcp_send_string(socket_fd, wt_msg);
        string ret("");
        return ret;
    }
    else{
        return wt_msg;
    }
    //Callee function will close the socket_fd;
}

bool wait_for_ack(int socket_fd){
    // struct timeval timeout_tv;
    // timeout_tv.tv_sec = WRITE_FC_TIMEOUT;      //in sec
    // timeout_tv.tv_usec = 0;
    // setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
    // char buf[MAX_BUF_LEN];
    // if(recv(socket_fd, buf, MAX_BUF_LEN, 0) == -1){
    //     cout << "wait_for_ack: NO receive ack!\n";
    //     close(socket_fd);
    //     return true;
    // }
    // else{
    //     cout << "wait_for_ack: Done!\n";
    //     close(socket_fd);
    //     return true;
    // }
    return true;
}

string handle_WA_msg(int socket_fd, string msg, bool need_to_send){
    cout << "Inside handle_WA_msg\n";
    int rep1 = string_to_int(msg.substr(2,2));
    int rep2 = string_to_int(msg.substr(4,2));
    int rep3 = string_to_int(msg.substr(6,2));
    int version = string_to_int(msg.substr(8,2));
    string file_name = msg.substr(10);

    file_table_lock.lock();

    if(filename_map.find(file_name) == filename_map.end()){ //File is not in file table
        // string msg("WS0");  //Reply that write operation failed
        cout << "handle_WA_msg: File is not in file table!\n";
        // if(need_to_send == true)
        //     tcp_send_string(socket_fd,msg);
        // else
        //     return msg;
    }
    else{
        set<int> rows = filename_map[file_name];
        for(auto it = rows.begin(); it != rows.end(); it++){
            if(file_table[*it].version > version){
                string msg ("WS1");         //Have a newer version. Tell client that write succeed
                if(need_to_send == true){
                    tcp_send_string(socket_fd,msg);
                    wait_for_ack(socket_fd);
                }
                file_table_lock.unlock();
                return msg;
            }
        }
        // //Check if the 3 reps are still the same
        // //If any of them failed or not the same, tell client that write failed
        // set<int> cur_reps;
        // for(auto it = rows.begin(); it != rows.end(); it ++){
        //     cur_reps.insert(file_table[*it].replica);
        // }
        // if(cur_reps.find(rep1) == cur_reps.end() || cur_reps.find(rep2) == cur_reps.end()  || cur_reps.find(rep3) == cur_reps.end() ){
        //     string msg ("WS0");         //Reply that write operation failed
        //     cout << "handle_WA_msg: Some replicas die!\n";
        //     if(need_to_send == true){
        //         send(socket_fd, msg.c_str(), msg.size(),0);
        //         wait_for_ack(socket_fd);
        //         //Need to wait for ack!??
        //     }
        //     file_table_lock.unlock();
        //     return msg;
        // }
    }

    ////////
    //Send msg to S1 and S2 to update its file table
    string fu_msg = create_FU_msg(file_name, rep1, rep2, rep3, version);

    VM_info rep1_info, rep2_info , rep3_info, master1_info, master2_info;
    //Send msg to rep1,rep2,rep3 to deliver file (move file from buffer to system folder)
    membership_list_lock.lock();
    if(membership_list.find(rep1) == membership_list.end() || membership_list.find(rep2) == membership_list.end() || membership_list.find(rep3) == membership_list.end()){
        membership_list_lock.unlock();
        string msg("WS0");                     //Reply that write operation failed. "WS0": write failed. "WS1": write succeed
        cout << "handle_WA_msg: Some replica failed. This write op is failed\n";
        if(need_to_send){
            send(socket_fd, msg.c_str(), msg.size(),0);
            wait_for_ack(socket_fd);
            //Need to wait for ack!??
        }
        file_table_lock.unlock();
        return msg;
    }
    else{
        rep1_info = vm_info_map[rep1];
        rep2_info = vm_info_map[rep2];
        rep3_info = vm_info_map[rep3];
        if(membership_list.find(master1_id) != membership_list.end()){
            master1_info = vm_info_map[master1_id];
        }
        else{
            master1_info.vm_num = -1;
        }

        if(membership_list.find(master2_id) != membership_list.end()){
            master2_info = vm_info_map[master2_id];
        }
        else{
            master2_info.vm_num = -1;
        }
        membership_list_lock.unlock();

        //Send msg to S1 and S2 to update their file table
        int master1_fd_sock;
        if(master1_info.vm_num != -1){
            master1_fd_sock = tcp_open_connection(master1_info.ip_addr_str, STABILIZATION_PORT);
            if(master1_fd_sock != -1){
                if(tcp_send_string(master1_fd_sock,fu_msg) == -1){
                    cout << "Cannot send file table update msg to master1\n";
                }
            }
            else{
                cout << "Cannot make connection to master1\n";
            }
        }
        int master2_fd_sock =-1;
        if(master2_info.vm_num != -1){
             master2_fd_sock = tcp_open_connection(master2_info.ip_addr_str, STABILIZATION_PORT);
            if(master2_fd_sock != -1){
                if(tcp_send_string(master2_fd_sock,fu_msg) == -1){
                    cout << "Cannot send file table update msg to master2\n";
                }
            }
            else{
                cout << "Cannot make connection to master2\n";
            }
        }
        string df_msg = create_DF_msg(file_name, version);

        int rep1_sock_fd = -1;
        //Send msg to rep1,rep2,rep3 to deliver file (move file from buffer to system folder)
        if(rep1 == my_vm_info.vm_num){
            //Add file to local file list && move file into buffer folder
            handle_DF_msg(df_msg);
        }
        else{
            rep1_sock_fd = tcp_open_connection(rep1_info.ip_addr_str, OP_PORT);
            if(rep1_sock_fd != -1){
                tcp_send_string(rep1_sock_fd, df_msg);
                //Need to wait for ACK. Or can move close down.
            }
            else{
                cout << "Cannot make connection to rep1\n";
            }
        }

        int rep2_sock_fd = -1;
        if(rep2 == my_vm_info.vm_num){
            //Add file to local file list && move file into buffer folder
            handle_DF_msg(df_msg);
        }
        else{
            rep2_sock_fd = tcp_open_connection(rep2_info.ip_addr_str, OP_PORT);
            if(rep2_sock_fd != -1){
                tcp_send_string(rep2_sock_fd, df_msg);
                //Need to wait for ACK. Or can move close down.
            }
            else{
                cout << "Cannot make connection to rep2\n";
            }
        }

        int rep3_sock_fd = -1;
        if(rep3 == my_vm_info.vm_num){
            //Add file to local file list && move file into buffer folder
            handle_DF_msg(df_msg);
        }
        else{
            int rep3_sock_fd = tcp_open_connection(rep3_info.ip_addr_str, OP_PORT);
            if(rep3_sock_fd != -1){
                tcp_send_string(rep3_sock_fd, df_msg);
            }
            else{
                cout << "Cannot make connection to rep3\n";
            }
        }

        //Update its file_table
        cout << "handle_WA_msg: Updating file table....\n";
        if(filename_map.find(file_name) != filename_map.end()){
            cout << "handle_WA_msg: File is in file table\n";
            set<int> rows = filename_map[file_name];
            for(auto it = rows.begin(); it != rows.end(); it++){
                file_table[*it].version = version;
            }
        }
        else{
            cout << "handle_WA_msg: Add new rows to file table\n";
            //Add new row to file table
            int temp1;
            if(file_table.size() > 0){
                 temp1 = file_table.rbegin()->first;
            }
            else{
                temp1 = 1;
            }
            file_row f_r1, f_r2, f_r3;
            f_r1.filename = file_name;
            f_r2.filename = file_name;
            f_r3.filename = file_name;
            f_r1.replica = rep1;
            f_r2.replica = rep2;
            f_r3.replica = rep3;
            f_r1.row = temp1+1;
            f_r2.row = temp1+2;
            f_r3.row = temp1+3;
            f_r1.version = version;
            f_r2.version = version;
            f_r3.version = version;
            file_table[temp1+1] = f_r1;
            file_table[temp1+2] = f_r2;
            file_table[temp1+3] = f_r3;
            set<int> temp_set;
            temp_set.insert(temp1+1);
            temp_set.insert(temp1+2);
            temp_set.insert(temp1+3);
            filename_map[file_name] = temp_set;
            if(replica_map.find(rep1) != replica_map.end()){
                set<int> t_s = replica_map[rep1];
                t_s.insert(temp1+1);
                replica_map[rep1] = t_s;
            }
            else{
                set<int> t_s;
                t_s.insert(temp1+1);
                replica_map[rep1] = t_s;
            }
            if(replica_map.find(rep2) != replica_map.end()){
                set<int> t_s = replica_map[rep2];
                t_s.insert(temp1+2);
                replica_map[rep2] = t_s;
            }
            else{
                set<int> t_s;
                t_s.insert(temp1+2);
                replica_map[rep2] = t_s;
            }
            if(replica_map.find(rep3) != replica_map.end()){
                set<int> t_s = replica_map[rep3];
                t_s.insert(temp1+3);
                replica_map[rep3] = t_s;
            }
            else{
                set<int> t_s;
                t_s.insert(temp1+3);
                replica_map[rep3] = t_s;
            }

            cout << "handle_WA_msg: Finish Adding new rows to file table\n";
        }

        file_table_lock.unlock();

        if(master2_fd_sock != -1)
            wait_for_ack(rep1_sock_fd);
        if(master1_fd_sock != -1)
            wait_for_ack(rep1_sock_fd);
        if(rep1_sock_fd != -1)
            wait_for_ack(rep1_sock_fd);
        if(rep2_sock_fd != -1)
            wait_for_ack(rep2_sock_fd);
        if(rep3_sock_fd != -1)
            wait_for_ack(rep3_sock_fd);

        // Reply to client
        string msg ("WS1");         //Tell client that write succeed
        if(need_to_send){
            send(socket_fd, msg.c_str(), msg.size(),0);
            wait_for_ack(socket_fd);
        }
        return msg;
    }
    ////
    file_table_lock.unlock();
    return "";
}


string create_FU_msg(string filename, int rep1, int rep2, int rep3, int version){
    string msg("FU");
    msg += int_to_string(rep1);
    msg += int_to_string(rep2);
    msg += int_to_string(rep3);
    msg += int_to_string(version);
    msg += filename;
    return msg;
}


string create_DF_msg(string file_name, int version){
    string msg("DF");
    msg += int_to_string(version);
    msg += file_name;
    return msg;
}


void handle_DF_msg(string msg){
    cout << "Inside handle_DF_msg\n";
    delivered_file_map_lock.lock();
    buffer_file_map_lock.lock();
    int version = string_to_int(msg.substr(2,2));
    string file_name = msg.substr(4);

    string file_name_in_buffer(file_name);
    file_name_in_buffer += msg.substr(2,2);

    if(buffer_file_map.find(file_name_in_buffer) == buffer_file_map.end()){
        cout << "Try to deliver unexist file in buffer. Something is wrong\n";
    }
    else{
        file_struct file = buffer_file_map[file_name_in_buffer];
        //If file exist, delete it
        if(delivered_file_map.find(file_name) != delivered_file_map.end()){
            if(delivered_file_map[file_name].version >= version){
                cout << "Try to deliver older version of file. Something is wrong\n";
                buffer_file_map_lock.unlock();
                delivered_file_map_lock.unlock();
                return;
            }
            remove(file_name.c_str());
            cout << "Inside DF_handler: remove file "<< file_name <<"\n";
            delivered_file_map.erase(file_name);
        }
        FILE* tempfp;
        if((tempfp=fopen(file_name_in_buffer.c_str(), "r")) == NULL){
            cout << "Inside DF_handler: file with name "<< file_name_in_buffer << "not available\n";
        }
        else{
            fclose(tempfp);
        }
        cout << "Inside DF_handler: rename " << file_name_in_buffer << " to " << file_name<<"\n";
        //Rename file in buffer to become file in delivered map
        if(rename(file_name_in_buffer.c_str(), file_name.c_str())){
            cout << "Inside DF_handler: Error when rename\n";
        }

        file_struct new_file_struct;
        new_file_struct.file_name = file_name;
        new_file_struct.version = version;
        //Add new file to delivered_file_map.
        delivered_file_map[file_name] = new_file_struct;

        //Erase file in buffer_file_map
        buffer_file_map.erase(file_name_in_buffer);

        //Delete all file in buffer with lower version than this version
        for(int i = 0; i < version; i++){
            string temp(file_name );
            temp += int_to_string(i);
            if(buffer_file_map.find(temp) != buffer_file_map.end()){
                remove(temp.c_str());
                buffer_file_map.erase(temp);
            }
        }
    }

    if(delivered_file_map.size() == 0){
        cout << "Inside DF_handler: delivered_file_map size = 0. Some thing is wrong\n";
    }
    else{
        for(auto it = delivered_file_map.begin(); it != delivered_file_map.end(); it++){
            cout << it->first<< " ";
        }
    }
    cout <<"\n";


    buffer_file_map_lock.unlock();
    delivered_file_map_lock.unlock();
}

bool handle_FC_msg(int socket_fd, string msg){
    delivered_file_map_lock.lock();
    cout << "Inside handle_FC_msg\n";

    int version = string_to_int(msg.substr(2,2));
    string file_name = msg.substr(4);

    string fcr_msg;
    bool flag;
    if(delivered_file_map.find(file_name) != delivered_file_map.end()
        && delivered_file_map[file_name].version >= version){
        fcr_msg= create_FCR_msg(false);
        cout << "Inside handle_FC_msg: " << "Not Accept file "<< fcr_msg<<"\n";
        send(socket_fd, fcr_msg.c_str(), fcr_msg.size(), 0);
        delivered_file_map_lock.unlock();
        return false;
    }

    fcr_msg= create_FCR_msg(true);
    cout << "Inside handle_FC_msg: " << "Accept file "<< fcr_msg<<"\n";
    send(socket_fd, fcr_msg.c_str(), fcr_msg.size(), 0);
    delivered_file_map_lock.unlock();
    return true;
}



void handle_FU_msg(string msg){
    
    file_table_lock.lock();
    int rep1, rep2,rep3;
    rep1 = string_to_int(msg.substr(2,2));
    rep2 = string_to_int(msg.substr(4,2));
    rep3 = string_to_int(msg.substr(6,2));
    int version = string_to_int(msg.substr(8,2));
    string file_name = msg.substr(10);
    cout << "Inside handle_FU_msg: "<< file_name<<"\n";
    
    if(filename_map.find(file_name) == filename_map.end()){
        cout << "Updating file table\n";
        next_version_map_lock.lock();
        if(next_version_map.find(file_name) == next_version_map.end() || next_version_map[file_name] < version){
            next_version_map[file_name] = version+1;
        }
        cout << "Here1\n";
        
        int row2_num,row1_num,row3_num;
        if(file_table.size() != 0){
            row1_num = file_table.rbegin()->first+1;
            row2_num = file_table.rbegin()->first+2;
            row3_num = file_table.rbegin()->first+3;
        }
        else{
            row1_num = 1;
            row2_num =2;
            row3_num = 3;
        }
        
        
        file_row row1, row2,row3;
        
        row1.filename = file_name;
        row1.version = version;
        row1.replica = rep1;
        row1.row = row1_num;
        
        row2.filename = file_name;
        row2.version = version;
        row2.replica = rep2;
        row2.row = row2_num;
        
        row3.filename = file_name;
        row3.version = version;
        row3.replica = rep3;
        row3.row = row3_num;
        
        file_table[row1_num] = row1;
        file_table[row2_num] = row2;
        file_table[row3_num] = row3;
        
        set<int> set_file_row;
        set_file_row.insert(row1_num);
        set_file_row.insert(row2_num);
        set_file_row.insert(row3_num);
        
        filename_map[file_name] =set_file_row;
        cout << "Inside FU_UPdateHere\n";
        
        set<int> set1 =replica_map[rep1];
        set1.insert(row1_num);
        replica_map[rep1] = set1;
        
        set<int> set2 =replica_map[rep2];
        set1.insert(row2_num);
        replica_map[rep2] = set2;
        
        set<int> set3 =replica_map[rep3];
        set1.insert(row3_num);
        replica_map[rep3] = set3;
        
        cout << "Done FU_UPdateHere\n";
        
        next_version_map_lock.unlock();
        file_table_lock.unlock();
        return;
    }
    
    
    set<int> rows = filename_map[file_name];
    for(auto it = rows.begin(); it != rows.end(); it++){
        if(file_table[*it].replica == rep1 || file_table[*it].replica == rep2 || file_table[*it].replica == rep3){
            if(file_table[*it].version >  version){
                cout << "Updated version is older than current version. Something's wrong!!!\n";
            }
            else{
                file_table[*it].version = version;
            }
        }
        else{
            cout << "Replica mismatch. Something's wrong!!!\n";
        }
    }
    file_table_lock.unlock();
    return;
}



bool receive_and_store_file(int socket_fd, string file_name){
    cout << "Inside receive_and_store_file\n";
    FILE* fp = fopen(file_name.c_str(), "w");

    int file_size = -1;
    struct timeval timeout_tv;
    timeout_tv.tv_sec = FILE_TRANS_TIMEOUT;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

    char buf[MAX_BUF_LEN];
    int numbytes;
    if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN,0)) <= 0){
        cout << "Inside receive_and_store_file: TIMEOUT1\n";
        fclose(fp);
        return false;
    }
    int count_temp ;
    string l_str("");
    for(count_temp= 0 ; count_temp < numbytes; count_temp++){
        if(buf[count_temp] == '\n'){
            break;
        }
        else{
            l_str.push_back(buf[count_temp]);
        }
    }

    file_size = stoi(l_str);
    cout << "Inside receive_and_store_file: file_size = " << file_size << "\n";

    if(file_size <= 0){
        return false;
    }

    if(count_temp < numbytes-1){
        fwrite((char*)(buf+count_temp+1), sizeof(char), numbytes-count_temp-1, fp);
        file_size -= numbytes-count_temp-1;
    }


    timeout_tv.tv_sec = 100;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

    int count = 0;
    while(file_size > 0){
        if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN,0)) < 0){
            fclose(fp);
            remove(file_name.c_str());
            cout << "Inside receive_and_store_file: TIMEOUT2: " <<file_size<< " " << count<<  "\n";
            return false;
        }
        else if(numbytes == 0){
            cout << "Inside receive_and_store_file: Connection close: " <<file_size<< "\n";
            break;
        }
        else{
            fwrite(buf, sizeof(char), numbytes, fp);
            file_size -= numbytes;
            count += numbytes;
            // cout << "Inside receive_and_store_file: Get some bytes" <<file_size<< " " << count<<  "\n";
        }
    }
    cout << "Number of byte remain = : "<< file_size << "\n";
    string ack_str("ACK");
    tcp_send_string(socket_fd, ack_str);

    cout << "Inside receive_and_store_file: Successfully get file\n";
    fclose(fp);

    return true;
}
