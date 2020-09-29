#ifndef __MEMORY_MANAGER_H__
#define __MEMORY_MANAGER_H__

#include <vector>
#include "parallel_defs.h"
#include "message.h"
#include "basic_task.h"
#include "task_graph.h"
#include "it_queue.h"

namespace apl
{

    class memory_manager
    {
    private:
        struct d_info
        {
            message* d;
            std::vector<message*> info;
            size_t version;
            MESSAGE_FACTORY_TYPE f_type;
            message_type type;
            message_id parent;
            std::vector<message_id> childs;
            bool created;
        };

        struct t_info
        {
            message_id m_id;
            perform_type type;
            task_id parent;
            size_t parents_count;
            size_t created_childs;
            std::vector<task_id> childs;
            std::vector<message_id> data;
            std::vector<message_id> const_data;
        };

        std::vector<t_info> task_v;
        std::vector<d_info> data_v;

    public:
        memory_manager();
        memory_manager(task_graph& _tg);
        ~memory_manager();

        void init(task_graph& _tg);

        std::queue<perform_id> get_ready_tasks();

        message_id add_message_init(message* ptr, message_type type, std::vector<message*>& info);
        message_id add_message_child(message* ptr, message_type type, message_id parent, std::vector<message*>& info);
        perform_id add_perform(message_id mes, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data);
        task_id add_task(task* ptr, task_type type, std::vector<message_id> data, std::vector<message_id> const_data, std::vector<message*>& info);

        message_id create_message_init(message_type type, std::vector<message*>& info);
        message_id create_message_child(message_type type, message_id parent, std::vector<message*>& info);
        task_id create_task(task_type type, std::vector<message_id> data, std::vector<message_id> const_data, std::vector<message*>& info);
        void include_child_to_parent(message_id child);
        void include_child_to_parent_recursive(message_id child);

        void add_message_init_with_id(message* ptr, message_id id, message_type type, std::vector<message*>& info);
        void add_message_child_with_id(message* ptr, message_id id, message_type type, message_id parent, std::vector<message*>& info);
        void add_perform_with_id(task_id id, perform_type type, const std::vector<message_id>& data, const std::vector<message_id>& const_data);
        void add_task_with_id(task* ptr, task_id id, task_type type, std::vector<message_id> data, std::vector<message_id> const_data, std::vector<message*>& info);

        void create_message_init_with_id(message_id id, message_type type, std::vector<message*>& info);
        void create_message_child_with_id(message_id id, message_type type, message_id parent, std::vector<message*>& info);
        void create_task_with_id(task_id id, task_type type, std::vector<message_id> data, std::vector<message_id> const_data, std::vector<message*>& info);

        void update_message_versions(message_id id);
        void update_version(message_id id, size_t new_version);

        void add_dependence(perform_id parent, perform_id child);
        void add_dependence(task_id parent, task_id child);

        void perform_task(perform_id id, task_environment& te);

        void set_message(message_id id, message* new_message);
        void set_message_type(message_id id, message_type new_type);
        void set_message_info(message_id id, std::vector<message*>& info);
        void set_message_parent(message_id id, message_id new_parent);
        void set_message_version(message_id id, size_t new_version);

        void set_task(task_id id, task* new_task);
        void set_task_type(task_id id, task_type new_type);
        void set_task_parent(task_id id, task_id new_parent);
        void set_task_parents_count(task_id id, size_t new_parents_count);
        void set_perform_created_childs(perform_id id, size_t new_created_childs);
        void set_task_created_childs(task_id id, size_t new_created_childs);
        void set_task_childs(task_id id, std::vector<task_id> new_childs);
        void set_task_data(task_id id, std::vector<message_id> new_data);
        void set_task_const_data(task_id id, std::vector<message_id> new_const_data);

        size_t message_count();
        message* get_message(message_id id);
        MESSAGE_FACTORY_TYPE get_message_factory_type(message_id id);
        message_type get_message_type(message_id id);
        std::vector<message*>& get_message_info(message_id id);
        message_id get_message_parent(message_id id);
        size_t get_message_version(message_id id);

        size_t task_count();
        task_id get_task_id(perform_id id);
        task* get_task(task_id id);
        perform_type get_perform_type(perform_id id);
        task_type get_task_type(task_id id);
        perform_id get_perform_parent(perform_id id);
        task_id get_task_parent(task_id id);
        size_t get_task_parents_count(task_id id);
        size_t get_perform_created_childs(perform_id id);
        size_t get_task_created_childs(task_id id);
        std::vector<task_id>& get_perform_childs(perform_id id);
        std::vector<task_id>& get_task_childs(task_id id);
        std::vector<message_id>& get_perform_data(perform_id id);
        std::vector<message_id>& get_task_data(task_id id);
        std::vector<message_id>& get_perform_const_data(perform_id id);
        std::vector<message_id>& get_task_const_data(task_id id);

        bool message_contained(message_id id);
        bool message_has_parent(message_id id);
        bool perform_has_parent(perform_id id);
        bool task_has_parent(task_id id);

        void delete_message(message_id id);
        void delete_task(task_id id);
        void delete_message_recursive(task_id id);
        void delete_task_recursive(task_id id);

        void clear();
    };

}

#endif // __MEMORY_MANAGER_H__
