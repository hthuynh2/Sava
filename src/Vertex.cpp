//#include "Vertex.h"
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//Vertex<VertexValue, EdgeValue, MessageValue>::Vertex(string vertex_id_){
//    vertex_id = vertex_id_;
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//const string& Vertex<VertexValue, EdgeValue, MessageValue>::get_vertex_id() const{
//    return vertex_id;
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//const VertexValue& Vertex<VertexValue, EdgeValue, MessageValue>::GetValue() const{
//    return vertex_value;
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//VertexValue* Vertex<VertexValue, EdgeValue, MessageValue>::MutableValue(){
//    return &vertex_value;
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//Graph_Base** Vertex<VertexValue, EdgeValue, MessageValue>::MutableGraph_ptr(){
//    return &graph_ptr;
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//class map<string, EdgeValue>::iterator Vertex<VertexValue, EdgeValue, MessageValue>::GetOutEdgeIterator_Start(){
//    return out_going_edges.begin();
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//class map<string, EdgeValue>::iterator Vertex<VertexValue, EdgeValue, MessageValue>::GetOutEdgeIterator_End(){
//    return out_going_edges.end();
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//int Vertex<VertexValue, EdgeValue, MessageValue>::get_out_edge_size(){
//    return out_going_edges.size();
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//void Vertex<VertexValue, EdgeValue, MessageValue>::SendMessageTo(const string& dest_vertex, const MessageValue& message){
//    graph_ptr->SendMessageTo(dest_vertex, message);
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//void Vertex<VertexValue, EdgeValue, MessageValue>::VoteToHalt(){
//    graph_ptr->VoteToHalt(vertex_id);
//}
//
////template <class VertexValue, class EdgeValue, class MessageValue>
////void Vertex<VertexValue, EdgeValue, MessageValue>::Reactivate(){
////    graph_ptr->Reactivate(vertex_id);
////}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//void Vertex<VertexValue, EdgeValue, MessageValue>::add_edge(string dest, EdgeValue weight){
//    //Might need lock
//    if(out_going_edges.find(dest) != out_going_edges.end()){
//        cout << "Vertex: add duplicate edge! Something is WRONG!\n";
//        return;
//    }
//    out_going_edges[dest] = weight;
//}
//
////VertexValue* Vertex::MutableId(){
////    return &vertex_id;
////}
//template <class VertexValue, class EdgeValue, class MessageValue>
//void Vertex<VertexValue, EdgeValue, MessageValue>::set_vertex_id(int vertex_id_){
//    vertex_id = vertex_id_;
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//void Vertex<VertexValue, EdgeValue, MessageValue>::add_edge(string dest){
//    out_going_edges[dest] = default_edge_val;
//}
//
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//int Vertex<VertexValue, EdgeValue, MessageValue>::superstep() const{
//    return graph_ptr->get_super_step();
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//void Vertex<VertexValue, EdgeValue, MessageValue>::send_to_all_neighbors(MessageValue& value){
//    for(auto it = out_going_edges.begin(); it!= out_going_edges.end(); it++){
//        SendMessageTo(it->first, value);
//    }
//    return;
//}
//
//template <class VertexValue, class EdgeValue, class MessageValue>
//void Vertex<VertexValue, EdgeValue, MessageValue>::setGraphPtr(Graph_Base* ptr){
//    graph_ptr = ptr;
//}
//
//
//
//
