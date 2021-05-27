#ifndef __MEMORY_MANAGER2_H__
#define __MEMORY_MANAGER2_H__

#include <vector>
#include <deque>
#include "apl/parallel_defs.h"
#include "apl/parallel_core.h"
#include "apl/message.h"
#include "apl/task.h"
#include "apl/task_graph.h"
#include "apl/containers/it_queue.h"
#include "apl/containers/vector_map.h"
#include "apl/parallelizer2/graphs.h"

namespace apl
{

    class memory_manager2
    {
    private:
        vector_map<message_id, memory_graph_node> mes_map;
        vector_map<perform_id, task_graph_node> task_map;
        vector_map<message_id, message_graph_node> mes_graph;
        std::set<graph_connection> ins, outs, child_ins, child_outs;

        std::set<message_id> messages_to_del;
        std::vector<perform_id> tasks_to_del;

        void inc_ref_count(message_id id);
        void dec_ref_count(message_id id);

        size_t base_mes_id = 0;
        size_t base_task_id = 0;

    public:
        memory_manager2();
        memory_manager2(task_graph& _tg, process main_proc);
        ~memory_manager2();

        void init(task_graph& _tg, process main_proc);

        std::deque<perform_id> get_ready_tasks(process owner);
        std::deque<perform_id> get_ready_tasks();
        std::set<message_id>& get_unreferenced_messages();
        std::set<message_id> get_messages_set();
        std::set<perform_id> get_performs_set();

        message_id add_message_init(message* ptr, message_type type, std::vector<message*>& info, process owner);
        message_id add_message_child(message* ptr, message_type type, message_id parent, std::vector<message*>& info, process owner);
        perform_id add_perform(message_id mes, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data, process owner);
        task_id add_task(task* ptr, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info, process owner);

        message_id create_message_init(message_type type, std::vector<message*>& info, process owner);
        message_id create_message_child(message_type type, message_id parent, std::vector<message*>& info, process owner);
        task_id create_task(task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info, process owner);
        void include_child_to_parent(message_id child);
        void include_child_to_parent_recursive(message_id child);

        void add_message_init_with_id(message* ptr, message_id id, message_type type, std::vector<message*>& info, process owner);
        void add_message_child_with_id(message* ptr, message_id id, message_type type, message_id parent, std::vector<message*>& info, process owner);
        void add_perform_with_id(task_id id, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data, process owner);
        void add_task_with_id(task* ptr, task_id id, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info, process owner);

        void create_message_init_with_id(message_id id, message_type type, std::vector<message*>& info, process owner);
        void create_message_child_with_id(message_id id, message_type type, message_id parent, std::vector<message*>& info, process owner);
        void create_task_with_id(task_id id, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info, process owner);

        void add_message_to_graph(message_id id, message_type type, process owner);
        void add_message_child_to_graph(message_id id, message_type type, message_id parent, process owner);

        void update_message_versions(perform_id id);
        void update_version(message_id id, size_t new_version);

        void add_dependence(perform_id parent, perform_id child);
        void add_dependence(task_id parent, task_id child);

        void send_message(message_id id, const intracomm& comm, process proc);
        void recv_message(message_id id, const intracomm& comm, process proc);
        void perform_task(perform_id id, task_environment& te);

        void set_message(message_id id, message* new_message);
        void set_message_type(message_id id, message_type new_type);
        void set_message_info(message_id id, std::vector<message*>& info);
        void set_message_parent(message_id id, message_id new_parent);
        void set_message_version(message_id id, size_t new_version);
        void set_message_child_state(message_id id, CHILD_STATE st);
        void insert_message_child(message_id id, message_id child);
        void erase_message_child(message_id id, message_id child);

        void set_task(task_id id, task* new_task);
        void set_task_type(task_id id, task_type new_type);
        void set_task_parent(task_id id, task_id new_parent);
        void set_perform_parents_count(perform_id id, size_t new_parents_count);
        void set_task_parents_count(task_id id, size_t new_parents_count);
        void set_perform_created_childs(perform_id id, size_t new_created_childs);
        void set_task_created_childs(task_id id, size_t new_created_childs);
        void set_task_childs(task_id id, std::vector<perform_id> new_childs);
        void set_task_data(task_id id, std::vector<message_id> new_data);
        void set_task_const_data(task_id id, std::vector<message_id> new_const_data);

        size_t ins_count() const;
        size_t outs_count() const;
        size_t child_ins_count() const;
        size_t child_outs_count() const;
        void connect_extern_in(perform_id out, perform_id in, size_t ext_proc);
        void connect_extern_child_out(perform_id out, perform_id in, size_t ext_proc);
        process find_connection_out_by_in_out(perform_id out, perform_id in) const;
        process find_connection_child_in_by_in_out(perform_id out, perform_id in) const;
        void erase_out(perform_id out, perform_id in, size_t ext_proc);
        void erase_child_in(perform_id out, perform_id in, size_t ext_proc);
        void insert_out(perform_id out, perform_id in, size_t ext_proc);
        void insert_task_graph_node(perform_id id, const task_graph_node& tgn);

        void erase_subgraph(const sub_graph& graph, size_t new_owner);

        void send_task_node(perform_id id, const intracomm& comm, process proc);
        void recv_task_node(perform_id id, const intracomm& comm, process proc);

        size_t message_count();
        message* get_message(message_id id);
        MESSAGE_FACTORY_TYPE get_message_factory_type(message_id id);
        message_type get_message_type(message_id id);
        std::vector<message*>& get_message_info(message_id id);
        message_id get_message_parent(message_id id);
        size_t get_message_version(message_id id);
        CHILD_STATE get_message_child_state(message_id id);
        std::set<message_id>& get_message_childs(message_id id);
        request_block& get_message_request_block(message_id id);
        request_block& get_message_info_request_block(message_id id);

        size_t task_count();
        task_id get_task_id(perform_id id);
        task* get_task(message_id id);
        task* get_task(task_id id);
        perform_type get_perform_type(perform_id id);
        task_type get_task_type(task_id id);
        perform_id get_perform_parent(perform_id id);
        task_id get_task_parent(task_id id);
        size_t get_perform_parents_count(perform_id id);
        size_t get_task_parents_count(task_id id);
        size_t get_perform_created_childs(perform_id id);
        size_t get_task_created_childs(task_id id);
        std::vector<perform_id>& get_perform_childs(perform_id id);
        std::vector<perform_id>& get_task_childs(task_id id);
        std::vector<message_id>& get_perform_data(perform_id id);
        std::vector<message_id>& get_task_data(task_id id);
        std::vector<message_id>& get_perform_const_data(perform_id id);
        std::vector<message_id>& get_task_const_data(task_id id);

        bool message_contained(message_id id) const;
        bool perform_contained(perform_id id) const;
        bool message_created(message_id id);
        bool message_has_parent(message_id id);
        bool perform_has_parent(perform_id id);
        bool task_has_parent(task_id id);

        void delete_message(message_id id);
        void delete_message_from_graph(message_id id);
        void delete_perform(perform_id id);
        void delete_task(task_id id);

        void clear();
    };

    class memory_manager_graph_adapter: public graph_adapter
    {
    public:
        memory_manager_graph_adapter(memory_manager2& mem);

        message_graph_node& get_message_node(message_id id) const override final;
        task_graph_node& get_task_node(perform_id id) const override final;
    };

}

#endif // __MEMORY_MANAGER2_H__