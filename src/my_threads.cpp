#include "operations.h"
#include "master_scheduler.h"

void op_handler_thread(int socket_fd){
    cout << "Inside op_handler_thread\n";
    int numbytes = 0;
    char buf[MAX_BUF_LEN];
    struct timeval timeout_tv;
    timeout_tv.tv_sec = 10;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
    if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0)) <= 0 ){
        close(socket_fd);
        cout << "op_handler_thread: Receive error!!\n";
        return;
    }
    else{
        string msg(buf, numbytes);
        if(strncmp(msg.c_str(), "RFT", 3) == 0){
            handle_RFT_msg(socket_fd, msg);
        }
        else if(strncmp(msg.c_str(), "RT", 2) == 0){
            cout << "op_handler_thread: Receive RT msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in read_at_client
        }
        else if(strncmp(msg.c_str(), "RS", 2) == 0){
            cout << "op_handler_thread: Receive RS msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in read_at_client
        }
        else if(strncmp(msg.c_str(), "RR", 2) == 0){
            cout << "op_handler_thread: Receive RR msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in master_thread
        }
        else if(strncmp(msg.c_str(), "WA", 2) == 0){
            cout << "op_handler_thread: Receive WA msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in write_at_client
        }
        else if(strncmp(msg.c_str(), "WR", 2) == 0){
            cout << "op_handler_thread: Receive WA msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in master_thread
        }
        else if(strncmp(msg.c_str(), "WT", 2) == 0){
            cout << "op_handler_thread: Receive WT msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in write_at_client
        }
        else if(strncmp(msg.c_str(), "WS", 2) == 0){
            cout << "op_handler_thread: Receive WS msg. Something is wrong\n";
            //This dont need to be handled here. This is handled in write_at_client
        }
        else if(strncmp(msg.c_str(), "FC", 2) == 0){
            if(msg.size() <= 4){
                cout << "op_handler_thread: " << "Error with msg FC\n";
            }
            else {
                if(handle_FC_msg(socket_fd, msg) == true){
                    string file_name = msg.substr(4);
                    int version = string_to_int(msg.substr(2,2));
                    string f_buffer_name(file_name);
                    f_buffer_name += int_to_string(version);

                    if(receive_and_store_file(socket_fd, f_buffer_name) == true){
                        cout << "op_handler_thread: " << "Receive file correctly" << file_name <<"\n";
                        buffer_file_map_lock.lock();
                        cout << "Inside op_handler_thread: copy to buffer\n";
                        file_struct f_str;
                        f_str.file_name = file_name;
                        f_str.version = version;
                        buffer_file_map[f_buffer_name] = f_str;
                        buffer_file_map_lock.unlock();
                    }
                    else{
                        cout << "op_handler_thread: " << "Error when Receive file.\n";
                    }
                }
            }
        }
        else if(strncmp(msg.c_str(), "DF", 2) == 0){    //Deliver file
            handle_DF_msg(msg);
        }
        else if(strncmp(msg.c_str(), "RFR", 2) == 0){   //Remove file
            handle_RFR_msg(msg);
        }
        else if(strncmp(msg.c_str(), "MS", 2) == 0){    //New for MP4
            cout << "op_handler_thread: Receive MS msg\n";
            handle_MS_msg(msg);
        }
        else if(strncmp(msg.c_str(), "MI", 2) == 0){    //New for MP4
            cout << "op_handler_thread: Receive MI msg\n";
            handle_MI_msg(msg);
        }
        else {
            cout << "op_handler_thread: " << "Received undefined msg:  "<< msg << "\n";
        }
    }
    close(socket_fd);
    return;
}


/* This is thread handler to read and handle msg
 *Input:    None
 *Return:   None
 */
 void op_listening_thread(){
    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage their_addr; // connector's address information
    socklen_t sin_size;


    int op_port_int= stoi(OP_PORT);
    string my_op_port = to_string(my_port_offset + op_port_int);

    cout << "OP listening on port "<< my_op_port<<"\n";

     string port = my_op_port;
     sockfd = tcp_bind_to_port(my_op_port);

     if (listen(sockfd, 10) == -1){
         perror("listen");
         exit(1);
     }
     while(1){
         sin_size = sizeof their_addr;
         new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
         if (new_fd == -1) {
			perror("accept");
			continue;
		 }
         thread new_thread = std::thread(op_handler_thread, new_fd);
         new_thread.detach();
         cout << "op_listening_thread: Receive new connection\n";
     }
}



void master_handler_thread(int socket_fd){
    cout << "Inside master_handler_thread\n";

    int numbytes = 0;
    char buf[MAX_BUF_LEN];
    struct timeval timeout_tv;
    timeout_tv.tv_sec = 10;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));
    if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0)) <= 0 ){
        close(socket_fd);
        cout << "master_handler_thread: Receive error!!\n";
        return;
    }
    else{
        string msg(buf, numbytes);
        if(strncmp(msg.c_str(), "WR", 2) == 0){
            cout << "master_handler_thread: " << "Hanlding WR msg\n";
            handle_WR_msg(socket_fd, msg, true);
        }
        else if(strncmp(msg.c_str(), "WA", 2) == 0){
            cout << "master_handler_thread: " << "Hanlding WA msg\n";
            handle_WA_msg(socket_fd, msg, true);
        }
        else if(strncmp(msg.c_str(), "RR", 2) == 0){
            cout << "master_handler_thread: " << "Hanlding RR msg\n";
            handle_RR_msg(socket_fd, msg, true);
        }
        else if(strncmp(msg.c_str(), "DR", 2) == 0){
            cout << "master_handler_thread: " << "Hanlding DR msg\n";
            handle_DR_msg(msg, socket_fd, true);
        }
        else if(strncmp(msg.c_str(), "LS", 2) == 0){
            cout << "master_handler_thread: " << "Hanlding DS msg\n";
            handle_LS_msg(socket_fd, msg, true);
        }
        else if(strncmp(msg.c_str(), "WU", 2) == 0){    //New for mp4
            cout << "master_handler_thread: " << "Hanlding WU msg\n";

            //NEED TO IMPLEMENT
        }
        else if(strncmp(msg.c_str(), "CZ", 2) == 0){    //New for mp4
            cout << "master_handler_thread: " << "Hanlding CZ msg\n";
            master_handle_cz_msg(msg);
        }
        else {
            cout << "master_handler_thread: " << "Received undefined msg:  "<< msg << "\n";
        }
    }
}

void master_listening_thread(){
    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
    struct addrinfo hints, *servinfo, *p;
    struct sockaddr_storage their_addr; // connector's address information
    socklen_t sin_size;

    string port = MASTER_PORT;
     sockfd = tcp_bind_to_port(MASTER_PORT);

    if (listen(sockfd, 10) == -1) {
        perror("listen");
        exit(1);
    }
    while(1){
        sin_size = sizeof their_addr;
        new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
        if (new_fd == -1) {
           perror("accept");
           continue;
        }
        thread new_thread(master_handler_thread, new_fd);
        new_thread.detach();
    }
}


void stabilization_handler_thread(int socket_fd){
    cout << "Inside stabilization_handler_thread\n";
    int numbytes = 0;
    char buf[MAX_BUF_LEN];
    struct timeval timeout_tv;
    timeout_tv.tv_sec = 10;      //in sec
    timeout_tv.tv_usec = 0;
    setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO,(struct timeval *)&timeout_tv,sizeof(struct timeval));

    if((numbytes = recv(socket_fd, buf, MAX_BUF_LEN, 0)) <= 0 ){
        close(socket_fd);
        cout << "Receive error!!\n";
        return;
    }
    else{
        string msg(buf, numbytes);
        if(strncmp(msg.c_str(), "FTR", 3) == 0){
            cout << "Stabilization_thread: " << "Handling FTR msg\n";
            handle_FTR_msg(socket_fd, msg, true);
        }
        else if(strncmp(msg.c_str(), "FC", 2) == 0){
            cout << "Stabilization_thread: " << "Handling FC msg\n";

            if(msg.size() <= 4){
                cout << "Stabilization_thread: " << "Error with msg FC\n";
            }
            else {
                ///////////////
                if(handle_FC_msg(socket_fd, msg) == true){
                    string file_name = msg.substr(4);
                    int version = string_to_int(msg.substr(2,2));
                    // string f_buffer_name(file_name);
                    // f_buffer_name += int_to_string(version);

                    if(receive_and_store_file(socket_fd, file_name) == true){
                        cout << "Stabilization_thread: " << "Receive file correctly" << file_name <<"\n";
                        // buffer_file_map_lock.lock();
                        file_table_lock.lock();
                        cout << "Inside Stabilization_thread: copy to delivered\n";
                        file_struct f_str;
                        f_str.file_name = file_name;
                        f_str.version = version;

                        delivered_file_map[file_name] = f_str;

                        file_table_lock.unlock();
                        // buffer_file_map_lock.unlock();
                    }
                    else{
                        cout << "Stabilization_thread: " << "Error when Receive file.\n";
                    }
                }
            }
        }
        else if(strncmp(msg.c_str(), "MU", 2) == 0){
            cout << "Stabilization_thread: " << "Handling MU msg\n";
            handle_MU_msg(msg);
        }
        else if(strncmp(msg.c_str(), "M", 1) == 0){
            cout << "Stabilization_thread: " << "Handling M msg\n";
            handle_M_msg(msg);
        }
        else if(strncmp(msg.c_str(), "FTD", 3) == 0){
            cout << "Stabilization_thread: " << "Handling FTD msg\n";
            //Need to check if this is master 1 or 2???
            handle_FTD_msg(msg);
        }
        else if(strncmp(msg.c_str(), "FU", 2) == 0){
            cout << "Stabilization_thread: " << "Handling FU msg\n";
            //Need to check if this is master 1 or 2???
            handle_FU_msg(msg);
        }
        else {
            cout << "Stabilization_thread: " << "Received undefined msg:  "<< msg << "\n";
        }
    }
    close(socket_fd);
    return;
}

/* This is thread handler to read and handle msg
 *Input:    None
 *Return:   None
 */
 void stabilization_listening_thread(){
     int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
     struct addrinfo hints, *servinfo, *p;
     struct sockaddr_storage their_addr; // connector's address information
     socklen_t sin_size;

     int stab_port_int= stoi(STABILIZATION_PORT);
     string my_stab_port = to_string(my_port_offset + stab_port_int);
     cout << "Stab listening on port "<< my_stab_port<<"\n";

     string port = my_stab_port;
      sockfd = tcp_bind_to_port(my_stab_port);

     if (listen(sockfd, 10) == -1) {
         perror("listen");
         exit(1);
     }
     while(1){
         sin_size = sizeof their_addr;
         new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
         if (new_fd == -1) {
			perror("accept");
			continue;
		 }
         thread new_thread(stabilization_handler_thread, new_fd);
         new_thread.detach();
     }
}
