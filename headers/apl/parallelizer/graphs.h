#ifndef ___APL_PARALLELIZER_GRAPHS_H__
#define ___APL_PARALLELIZER_GRAPHS_H__

#include <vector>
#include "apl/parallel_defs.h"
#include "apl/request_container.h"
#include "apl/intracomm.h"
#include "apl/containers/vector_map.h"
#include "apl/task_graph.h"

namespace apl
{

    constexpr const message_id MESSAGE_ID_UNDEFINED = {std::numeric_limits<size_t>::max(), MPI_PROC_NULL};
    constexpr const perform_id PERFORM_ID_UNDEFINED = {std::numeric_limits<size_t>::max(), MPI_PROC_NULL};
    constexpr const task_id TASK_ID_UNDEFINED = {MESSAGE_ID_UNDEFINED, PERFORM_ID_UNDEFINED};

    struct processes_group
    {
        size_t size;
        process head;
        process owner;
    };

    enum class CREATION_STATE: size_t
    {
        UNDEFINED,
        REFFERED,
        WAITING,
        CREATED,
        CHILD
    };

    class memory_graph_node
    {
    private:
        message* d = nullptr;
        request_block d_req;
        std::vector<message*> info;
        request_block info_req;
        size_t version = 0;
        CREATION_STATE c_type = CREATION_STATE::UNDEFINED;

    public:
        memory_graph_node() = default;
        memory_graph_node(memory_graph_node&& src) = default;
        ~memory_graph_node() = default;
        memory_graph_node& operator=(memory_graph_node&& src) = default;

        void send_message(const intracomm& comm, process proc);
        void recv_message(const intracomm& comm, process proc);
        void send_info(const intracomm& comm, process proc);
        void recv_info(const intracomm& comm, process proc);
    };

    class message_graph_node
    {

    };

    class task_graph_node
    {
    private:
        message_id m_id = MESSAGE_ID_UNDEFINED;
        perform_type type = PERFORM_TYPE_UNDEFINED;
        perform_id parent = PERFORM_ID_UNDEFINED;
        size_t parents_count = 0;
        size_t created_childs = 0;
        std::vector<perform_id> childs;
        std::vector<message_id> data;
        std::vector<message_id> const_data;

    public:
        task_graph_node() = default;
        task_graph_node(task_graph_node&& src) = default;
        ~task_graph_node() = default;
        task_graph_node& operator=(task_graph_node&& src) = default;

    };

    class graph_adapter
    {
    public:

        virtual message_graph_node& get_message_node(message_id id) const = 0;
        virtual task_graph_node& get_task_node(perform_id id) const = 0;
    };

    struct graph_connection
    {
        perform_id out;
        perform_id in;
        size_t external_group_id;
    };

    template<>
    struct simple_datatype_map<graph_connection>
    {
        using map = type_map
        <
            type_offset<perform_id, offsetof(graph_connection, out)>,
            type_offset<perform_id, offsetof(graph_connection, in)>,
            type_offset<size_t, offsetof(graph_connection, external_group_id)>
        >;
    };

    struct sub_graph
    {
        std::vector<perform_id> tasks;
        std::vector<graph_connection> ins, outs;
    };

    class graph_analizer
    {
    private:
        const graph_adapter* adapter;

    public:
        graph_analizer(const graph_adapter& a);

        std::vector<perform_id> get_subgraph(size_t prefered_size) const;
        std::vector<graph_connection> split_graph(const std::vector<processes_group>& groups) const;

    };

}

#endif // ___APL_PARALLELIZER_GRAPHS_H__
