#include "UDP.h"

int tcp_open_connection(string dest_ip, string dest_port){
    if(dest_port != MASTER_PORT){
        cout << "connecting to non-master port\n";
        int temp = port_map[dest_ip];
        int port_int = stoi(dest_port);
        dest_port = to_string(temp + port_int);
        cout << "Connecting to port "<< dest_port<<"\n";
        for (auto it = port_map.begin(); it != port_map.end(); it++){
            cout << it->first << " "<< it->second << "||||||";
        }
        cout <<"\n";
    }

    int sockfd, numbytes;
    char buf[MAX_BUF_LEN];
    struct addrinfo hints, *servinfo, *p;
    int rv;
    char s[INET6_ADDRSTRLEN];


    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if ((rv = getaddrinfo(dest_ip.c_str(), dest_port.c_str(), &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return -1;
    }

    // loop through all the results and connect to the first we can
    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype,
                             p->ai_protocol)) == -1) {
            perror("client: socket");
            continue;
        }
        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            perror("client: connect");
            continue;
        }
        break;
    }

    if (p == NULL) {
        fprintf(stderr, "client: failed to connect\n");
        return -1;
    }

    freeaddrinfo(servinfo); // all done with this structure
    return sockfd;
}

int tcp_send_string(int sockfd, string& str){
    int msg_len = (int) str.size();
    int offset = 0;
    while(msg_len > 0){
        int numbyte = send(sockfd,(void*) (str.c_str() + offset), msg_len, 0);
        if(numbyte == -1){
            cout << "handle_input_line: Cannot send\n";
            return false;
        }
        msg_len -= numbyte;
        offset += numbyte;
    }
    return 0;
}


int tcp_send_string_with_size(int sockfd, string& str_){
    string str("");
    str += to_string(str_.size()) + " ";
    str += str_;
    
    total_send += str_.size();
    
    int msg_len = (int) str.size();
//    cout << "tcp_send_string_with_size: msg_len : " << msg_len<<"\n";
//    cout <<str;
//    cout << "tcp_send_string_with_size: New msg = " << str <<"\n";
    int offset = 0;
    while(msg_len > 0){
        int numbyte = send(sockfd,(void*) (str.c_str() + offset), msg_len, 0);
        if(numbyte == -1){
            cout << "tcp_send_string_with_size: Cannot send\n";
            return -1;
        }
        msg_len -= numbyte;
        offset += numbyte;
    }
    return 0;
}

string tcp_receive_str(int sockfd){
    //Get size
    int msg_size = 0;
    unsigned char c[1];
    int temp_numbytes;
    string ret_str("");
    while(1){
        if((temp_numbytes = recv(sockfd, c, 1, 0)) <= 0){
            //            cout << "Inside tcp_receive_str: Receive error 1!!\n";
            return ret_str;
        }
        else{
            //            cout << hex << "tcp_receive_str: 0x" << (unsigned int)c[0] <<endl;
            //            cout << "tcp_receive_str: " << c[0];
            if(c[0] == ' '){
                break;
            }
            if(c[0] > '9' || c[0] < '0'){
                cout << "Inside tcp_receive_str: Receive error 2!!" << c[0] << "e\n";
                return ret_str;
            }
            msg_size = msg_size* 10 + c[0] -'0';
        }
    }
//    cout << "tcp_receive_str: msg_size = " <<msg_size<<"\n";
    if(msg_size <=0){
        return ret_str;
    }
    
    //Get msg
    char buf[MAX_BUF_LEN*2];
    int numbyte;
    int offset = 0;
    int msg_len = msg_size;
    while(msg_len > 0){
        int numbyte = recv(sockfd,(void*) (buf + offset), msg_len, 0);
        if(numbyte == -1){
            cout << "handle_input_line: Cannot send\n";
            return ret_str;
        }
        msg_len -= numbyte;
        offset += numbyte;
    }
    
    total_receive += msg_size;
    
    string str(buf, msg_size);
    //    cout << "tcp_receive_str: get " << str<<"\n";
    return str;
}


//int tcp_close_connection(int sockfd){
//    if(sockfd != -1)
//        close(sockfd);
//}

//This function used to receive only ONE short msg!!! Not to transfer file!!
// int tcp_receive_short_msg(int sockfd, char* buf){  //Receive msg less than MAX_BUF_LEN
//     fd_set r_master, r_fds;
//     FD_ZERO(&r_fds);
//     FD_SET(sockfd, &r_master);
//     int numbytes = 0;
//
//     while(1){
//         r_fds = r_master;
//         if(select(sockfd+1, &r_fds, NULL, NULL, NULL ) == -1){
//             return -1;
//         }
//         if(FD_ISSET(sockfd, &r_fds)){
//             if ((numbytes = recv(sockfd, buf, MAX_BUF_LEN, 0)) <= 0){
//                 perror("Receive error!!\n");
//                 close(sockfd);
//                 return -1 ;
//             }
//             return numbytes;
//         }
//
//         membership_list_lock.lock();
//         if(membership_list.find(node_id) == membership_list.end()){
//             membership_list_lock.unlock();
//             close(sockfd);
//             return -1;
//         }
//         membership_list_lock.unlock();
//     }
//     return -1;
// }


int tcp_bind_to_port(string port){
    int sockfd, new_fd;  // listen on sock_fd, new connection on new_fd
	struct addrinfo hints, *servinfo, *p;
	struct sockaddr_storage their_addr; // connector's address information
	socklen_t sin_size;
	struct sigaction sa;
	int yes=1;
	char s[INET6_ADDRSTRLEN];
	int rv;

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE; // use my IP

	if ((rv = getaddrinfo(NULL, port.c_str(), &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return 1;
	}

	// loop through all the results and bind to the first we can
	for(p = servinfo; p != NULL; p = p->ai_next) {
		if ((sockfd = socket(p->ai_family, p->ai_socktype,
				p->ai_protocol)) == -1) {
			perror("server: socket");
			continue;
		}

		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes,
				sizeof(int)) == -1) {
			perror("setsockopt");
			exit(1);
		}

		if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
			close(sockfd);
			perror("server: bind");
			continue;
		}
		break;
	}

	if (p == NULL)  {
		fprintf(stderr, "server: failed to bind\n");
		return 2;
	}

	freeaddrinfo(servinfo); // all done with this structure
    return sockfd;

}
