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
        size_t size = 0;
        process head = MPI_PROC_NULL;
        process owner = MPI_PROC_NULL;
        process internal_head = MPI_PROC_NULL;
        intracomm comm;
    };

    enum class CREATION_STATE: size_t
    {
        UNDEFINED,
        REFFERED,
        WAITING,
        CREATED,
        CHILD
    };

    enum class CHILD_STATE: size_t
    {
        UNDEFINED,
        INCLUDED,
        NEWER
    };

    class memory_graph_node
    {
    public:
        message* d = nullptr;
        request_block d_req;
        std::vector<message*> info;
        request_block info_req;
        size_t version = 0;
        CREATION_STATE c_type = CREATION_STATE::UNDEFINED;

    public:
        memory_graph_node() = default;
        memory_graph_node(memory_graph_node&& src) noexcept = default;
        ~memory_graph_node() = default;
        memory_graph_node& operator=(memory_graph_node&& src) = default;

        void send_message(const intracomm& comm, process proc);
        void recv_message(const intracomm& comm, process proc);
        void send_info(const intracomm& comm, process proc);
        void recv_info(const intracomm& comm, process proc);
    };

    class message_graph_node
    {
    public:
        message_type type = MESSAGE_TYPE_UNDEFINED;
        MESSAGE_FACTORY_TYPE f_type = MESSAGE_FACTORY_TYPE::UNDEFINED;
        message_id parent = MESSAGE_ID_UNDEFINED;
        std::set<message_id> childs;
        size_t refs_count = 0;
        CHILD_STATE ch_state = CHILD_STATE::UNDEFINED;
        process owner = MPI_PROC_NULL;

    public:
        message_graph_node() = default;
        message_graph_node(message_graph_node&& src) noexcept = default;
        ~message_graph_node() = default;
        message_graph_node& operator=(message_graph_node&& src) = default;
    };

    template<>
    void sender::send<message_graph_node>(const message_graph_node& src) const;

    template<>
    void receiver::recv<message_graph_node>(message_graph_node& dst) const;

    class task_graph_node
    {
    public:
        message_id m_id = MESSAGE_ID_UNDEFINED;
        perform_type type = PERFORM_TYPE_UNDEFINED;
        perform_id parent = PERFORM_ID_UNDEFINED;
        size_t parents_count = 0;
        size_t created_childs_count = 0;
        std::vector<perform_id> childs_v;
        std::vector<message_id> data;
        std::vector<message_id> const_data;
        process owner = MPI_PROC_NULL;

    public:
        task_graph_node() = default;
        task_graph_node(const task_graph_node& src) = default;
        task_graph_node(task_graph_node&& src) noexcept = default;
        ~task_graph_node() = default;
        task_graph_node& operator=(task_graph_node&& src) = default;
        task_graph_node& operator=(const task_graph_node& src) = default;

    };
    
    template<>
    void sender::send<task_graph_node>(const task_graph_node& src) const;

    template<>
    void receiver::recv<task_graph_node>(task_graph_node& dst) const;

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

        bool operator<(const graph_connection& oth) const;
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
        std::vector<graph_connection> ins, outs, child_ins, child_outs;
    };

    template<>
    void sender::send<sub_graph>(const sub_graph& src) const;

    template<>
    void receiver::recv<sub_graph>(sub_graph& dst) const;

    struct group_info
    {
        size_t active_tasks;
        size_t all_tasks;
    };

    class graph_analizer
    {
    private:
        const graph_adapter* adapter;

    public:
        graph_analizer(const graph_adapter& a);

        bool need_split(const std::vector<processes_group>& groups, const std::vector<group_info>& info, size_t ready_tasks_size) const;
        std::vector<perform_id> get_subgraph(size_t prefered_size) const;
        std::vector<sub_graph> split_graph(const std::vector<processes_group>& groups, const std::vector<group_info>& info, std::deque<perform_id>& ready_tasks) const;

    };

}

#endif // ___APL_PARALLELIZER_GRAPHS_H__
