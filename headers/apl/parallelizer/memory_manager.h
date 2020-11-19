#ifndef __MEMORY_MANAGER_H__
#define __MEMORY_MANAGER_H__

#include <vector>
#include "apl/parallel_defs.h"
#include "apl/message.h"
#include "apl/task.h"
#include "apl/task_graph.h"
#include "apl/containers/it_queue.h"
#include "apl/containers/vector_map.h"

namespace apl
{

    constexpr const message_id MESSAGE_ID_UNDEFINED = {std::numeric_limits<size_t>::max(), MPI_PROC_NULL};
    constexpr const perform_id PERFORM_ID_UNDEFINED = {std::numeric_limits<size_t>::max(), MPI_PROC_NULL};
    constexpr const task_id TASK_ID_UNDEFINED = {MESSAGE_ID_UNDEFINED, PERFORM_ID_UNDEFINED};

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

    class memory_manager
    {
    private:
        struct d_info
        {
            message* d = nullptr;
            std::vector<message*> info;
            size_t version;
            CREATION_STATE c_type;
        };

        struct t_info
        {
            message_id m_id = MESSAGE_ID_UNDEFINED;
            perform_type type = PERFORM_TYPE_UNDEFINED;
            perform_id parent = PERFORM_ID_UNDEFINED;
            size_t parents_count = 0;
            size_t created_childs = 0;
            std::vector<perform_id> childs;
            std::vector<message_id> data;
            std::vector<message_id> const_data;
        };

        struct message_graph
        {
            message_type type = MESSAGE_TYPE_UNDEFINED;
            MESSAGE_FACTORY_TYPE f_type = MESSAGE_FACTORY_TYPE::UNDEFINED;
            message_id parent = MESSAGE_ID_UNDEFINED;
            std::set<message_id> childs;
            size_t refs_count = 0;
            CHILD_STATE ch_state;
        };

        vector_map<message_id, d_info> mes_map;
        vector_map<perform_id, t_info> task_map;
        vector_map<message_id, message_graph> mes_graph;

        std::set<message_id> messages_to_del;
        std::vector<perform_id> tasks_to_del;

        void inc_ref_count(message_id id);
        void dec_ref_count(message_id id);

        size_t base_mes_id = 0;
        size_t base_task_id = 0;

    public:
        memory_manager();
        memory_manager(task_graph& _tg);
        ~memory_manager();

        void init(task_graph& _tg);

        std::queue<perform_id> get_ready_tasks();
        std::set<message_id>& get_unreferenced_messages();
        std::set<message_id> get_messages_set();
        std::set<perform_id> get_performs_set();

        message_id add_message_init(message* ptr, message_type type, std::vector<message*>& info);
        message_id add_message_child(message* ptr, message_type type, message_id parent, std::vector<message*>& info);
        perform_id add_perform(message_id mes, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data);
        task_id add_task(task* ptr, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info);

        message_id create_message_init(message_type type, std::vector<message*>& info);
        message_id create_message_child(message_type type, message_id parent, std::vector<message*>& info);
        task_id create_task(task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info);
        void include_child_to_parent(message_id child);
        void include_child_to_parent_recursive(message_id child);

        void add_message_init_with_id(message* ptr, message_id id, message_type type, std::vector<message*>& info);
        void add_message_child_with_id(message* ptr, message_id id, message_type type, message_id parent, std::vector<message*>& info);
        void add_perform_with_id(task_id id, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data);
        void add_task_with_id(task* ptr, task_id id, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info);

        void create_message_init_with_id(message_id id, message_type type, std::vector<message*>& info);
        void create_message_child_with_id(message_id id, message_type type, message_id parent, std::vector<message*>& info);
        void create_task_with_id(task_id id, task_type type, std::vector<message_id>& data, std::vector<message_id>& const_data, std::vector<message*>& info);

        void add_message_to_graph(message_id id, message_type type);
        void add_message_child_to_graph(message_id id, message_type type, message_id parent);

        void update_message_versions(perform_id id);
        void update_version(message_id id, size_t new_version);

        void add_dependence(perform_id parent, perform_id child);
        void add_dependence(task_id parent, task_id child);

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

        size_t message_count();
        message* get_message(message_id id);
        MESSAGE_FACTORY_TYPE get_message_factory_type(message_id id);
        message_type get_message_type(message_id id);
        std::vector<message*>& get_message_info(message_id id);
        message_id get_message_parent(message_id id);
        size_t get_message_version(message_id id);
        CHILD_STATE get_message_child_state(message_id id);
        std::set<message_id>& get_message_childs(message_id id);

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

        bool message_contained(message_id id);
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

}

#endif // __MEMORY_MANAGER_H__
