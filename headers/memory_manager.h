#ifndef __MEMORY_MANAGER_H__
#define __MEMORY_MANAGER_H__

#include <vector>
#include "message.h"
#include "basic_task.h"
#include "task_graph.h"
#include "it_queue.h"
#include "parallel_defs.h"

namespace auto_parallel
{

    class memory_manager
    {
    private:
        struct d_info
        {
            message* d;
            size_t type;
            message::init_info_base* iib;
            message::part_info_base* pib;
            message_id parent;
            size_t version;
            bool created;
        };

        struct t_info
        {
            task* t;
            size_t type;
            task_id parent;
            size_t parents_count;
            size_t created_childs;
            std::vector<task_id> childs;
            std::vector<message_id> data;
            std::vector<message_id> const_data;
            bool created;
        };

        std::vector<t_info> task_v;
        std::vector<d_info> data_v;

    public:
        memory_manager();
        memory_manager(task_graph& _tg);
        ~memory_manager();

        void init(task_graph& _tg);

        std::queue<task_id> get_ready_tasks();

        message_id add_message(message* ptr, size_t type = std::numeric_limits<size_t>::max());
        task_id add_task(task* ptr, size_t type = std::numeric_limits<size_t>::max());

        message_id create_message(size_t type);
        message_id create_message(size_t type, message::init_info_base* iib);
        message_id create_message(size_t type, message_id parent, message::part_info_base* pib, message::init_info_base* iib = nullptr);
        task_id create_task(size_t type, std::vector<message_id> data, std::vector<message_id> const_data);

        void create_message_with_id(message_id id, size_t type);
        void create_message_with_id(message_id id, size_t type, message::init_info_base* iib);
        void create_message_with_id(message_id id, size_t type, message_id parent, message::part_info_base* pib, message::init_info_base* iib = nullptr);
        void create_task_with_id(task_id id, size_t type, std::vector<message_id> data, std::vector<message_id> const_data);

        void update_message_versions(task_id id);
        void update_version(message_id id, size_t new_version);

        void add_dependence(task_id parent, task_id child);

        void set_message(message_id id, message* new_message);
        void set_message_type(message_id id, size_t new_type);
        void set_message_init_info(message_id id, message::init_info_base* new_iib);
        void set_message_part_info(message_id id, message::part_info_base* new_pib);
        void set_message_parent(message_id id, size_t new_parent);
        void set_message_version(message_id id, size_t new_version);

        void set_task(task_id id, task* new_task);
        void set_task_type(task_id id, size_t new_type);
        void set_task_parent(task_id id, size_t new_parent);
        void set_task_parents_count(task_id id, size_t new_parents_count);
        void set_task_created_childs(task_id id, size_t new_created_childs);
        void set_task_childs(task_id id, std::vector<task_id> new_childs);
        void set_task_data(task_id id, std::vector<message_id> new_data);
        void set_task_const_data(task_id id, std::vector<message_id> new_const_data);

        size_t message_count();
        message* get_message(message_id id);
        size_t get_message_type(message_id id);
        message::init_info_base* get_message_init_info(message_id id);
        message::part_info_base* get_message_part_info(message_id id);
        size_t get_message_parent(message_id id);
        size_t get_message_version(message_id id);

        size_t task_count();
        task* get_task(task_id id);
        size_t get_task_type(task_id id);
        size_t get_task_parent(task_id id);
        size_t get_task_parents_count(task_id id);
        size_t get_task_created_childs(task_id id);
        std::vector<task_id>& get_task_childs(task_id id);
        std::vector<message_id>& get_task_data(task_id id);
        std::vector<message_id>& get_task_const_data(task_id id);

        bool message_has_parent(message_id id);
        bool task_has_parent(task_id id);

        void delete_message(message_id id);
        void delete_task(task_id id);
        void delete_message_recursive(task_id id);
        void delete_task_recursive(task_id id);

        void clear();
    };

}

#endif // __MEMORY_MANAGER_H__
