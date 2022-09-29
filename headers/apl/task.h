#ifndef __BASIC_TASK_H__
#define __BASIC_TASK_H__

#include <vector>
#include "apl/parallel_defs.h"
#include "apl/message.h"
#include "apl/message_factory.h"

namespace apl
{

    typedef size_t task_type;

    constexpr const task_type TASK_TYPE_UNDEFINED = std::numeric_limits<task_type>::max();

    enum class MESSAGE_SOURCE: size_t
    {
        UNDEFINED, GLOBAL,
        TASK_ARG, TASK_ARG_C,
        REFERENCE,
        INIT, CHILD,
        INIT_A, CHILD_A
    };

    enum class TASK_SOURCE: size_t
    {
        UNDEFINED, GLOBAL,
        INIT, REFERENCE,
        CHILD
    };

    struct local_message_id
    {
        size_t id;
        MESSAGE_SOURCE src;
    };

    struct local_task_id
    {
        local_message_id mes;
        size_t id;
        TASK_SOURCE src;
    };

    template<typename Type>
    struct mes_id
    {
        size_t id;
        MESSAGE_SOURCE src;

        typedef Type type;

        mes_id();
        mes_id(local_message_id id);
        mes_id(const mes_id<Type>& id);

        mes_id<const Type> as_const();
        operator local_message_id();
    };

    template<typename Type>
    mes_id<Type>::mes_id(): id(std::numeric_limits<size_t>::max()), src(MESSAGE_SOURCE::UNDEFINED)
    { }

    template<typename Type>
    mes_id<Type>::mes_id(local_message_id id): id(id.id), src(id.src)
    { }

    template<typename Type>
    mes_id<Type>::mes_id(const mes_id<Type>& id): id(id.id), src(id.src)
    { }

    template<typename Type>
    mes_id<const Type> mes_id<Type>::as_const()
    {
        mes_id<const Type> i({id, src});
        return i;
    }

    template<typename Type>
    mes_id<Type>::operator local_message_id()
    {
        return {id, src};
    }

    struct task_dependence
    {
        local_task_id parent;
        local_task_id child;
    };

    // local_message_id
    template<>
    struct simple_datatype_map<local_message_id>
    { using map = type_map<type_offset<size_t, offsetof(local_message_id, id)>, type_offset<size_t, offsetof(local_message_id, src)>>; };

    // local_task_id
    template<>
    struct simple_datatype_map<local_task_id>
    {
        using map = type_map
        <
            type_offset<local_message_id, offsetof(local_task_id, mes)>,
            type_offset<size_t, offsetof(local_task_id, id)>,
            type_offset<size_t, offsetof(local_task_id, src)>
        >;
    };

    // task_dependence
    template<>
    struct simple_datatype_map<task_dependence>
    { using map = type_map<type_offset<local_task_id, offsetof(task_dependence, parent)>, type_offset<local_task_id, offsetof(task_dependence, child)>>; };

    class task;

    struct message_init_data
    {
        message_type type = MESSAGE_TYPE_UNDEFINED;
        std::vector<message*> ii;
    };

    struct message_init_add_data
    {
        message_type type = MESSAGE_TYPE_UNDEFINED;
        std::vector<message*> ii;
        message* mes = nullptr;
    };

    struct message_child_data
    {
        message_type type = MESSAGE_TYPE_UNDEFINED;
        local_message_id sourse {};
        std::vector<message*> pi;
    };

    struct message_child_add_data
    {
        message_type type = MESSAGE_TYPE_UNDEFINED;
        local_message_id sourse {};
        std::vector<message*> pi;
        message* mes = nullptr;
    };

    struct task_data
    {
        task_type type = TASK_TYPE_UNDEFINED;
        std::vector<local_message_id> data, c_data;
    };

    class task_environment: public message
    {
    private:

        size_t processes_count;
        size_t threads_count;
        task_data this_task;
        std::vector<local_message_id> all_task_data;
        local_task_id this_task_id;

        std::vector<local_message_id> created_messages_v;
        std::vector<message_init_data> messages_init_v;
        std::vector<message_init_add_data> messages_init_add_v;
        std::vector<message_child_data> messages_childs_v;
        std::vector<message_child_add_data> messages_childs_add_v;

        std::vector<local_task_id> created_tasks_v;
        std::vector<task_data> tasks_v;
        std::vector<task_data> tasks_child_v;

        std::vector<task_dependence> dependence_v;

        void set_all_task_data();

    public:

        task_environment();
        task_environment(task_data& td);
        task_environment(task_data&& td);
        task_environment(task_type type, size_t args_count, size_t const_args_count, size_t _processes_count, size_t _threads_count);

        local_message_id create_message_init(message_type type, const std::vector<message*>& info);
        local_message_id create_message_child(message_type type, local_message_id source, const std::vector<message*>& info);

        local_message_id add_message_init(message_type type, message* m, const std::vector<message*>& info);
        local_message_id add_message_child(message_type type, message* m, local_message_id source, const std::vector<message*>& info);

        local_task_id create_task(message_type m_type, task_type type, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data, const std::vector<message*>& info);
        local_task_id create_child_task(message_type m_type, task_type type, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data, const std::vector<message*>& info);

        local_task_id add_task(task_type type, local_message_id t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);
        local_task_id add_child_task(task_type type, local_message_id t, const std::vector<local_message_id>& data, const std::vector<local_message_id>& const_data);

        void add_dependence(local_task_id parent, local_task_id child);

        std::vector<local_task_id>& result_task_ids();
        std::vector<task_data>& created_tasks_simple();
        std::vector<task_data>& created_child_tasks();

        std::vector<local_message_id>& result_message_ids();
        std::vector<message_init_data>& created_messages_init();
        std::vector<message_child_data>& created_messages_child();
        std::vector<message_init_add_data>& added_messages_init();
        std::vector<message_child_add_data>& added_messages_child();

        std::vector<task_dependence>& created_dependences();

        local_message_id arg_id(size_t n) const;
        task_data get_this_task_data() const;
        local_task_id get_this_task_id() const;

        void set_workers_count(size_t _processes_count, size_t _threads_count);
        size_t get_workers_count() const;
        size_t get_processes_count() const;
        size_t get_threads_count() const;

        void send(const sender& se) const override;
        void recv(const receiver& re) override;
        void isend(const sender& se, request_block& req) const override;
        void irecv(const receiver& re, request_block& req) override;

    };

    template<typename Type>
    struct new_task_id
    {
        mes_id<Type> m_id;
        size_t id;
        TASK_SOURCE src;

        typedef Type type;

        new_task_id(local_task_id id);

        new_task_id<const Type> as_const();
        operator local_task_id();
    };

    template<typename Type>
    new_task_id<Type>::new_task_id(local_task_id id): m_id(id.mes), id(id.id), src(id.src)
    { }

    template<typename Type>
    new_task_id<const Type> new_task_id<Type>::as_const()
    {
        new_task_id<const Type> i{m_id, id, src};
        return i;
    }

    template<typename Type>
    new_task_id<Type>::operator local_task_id()
    { return {m_id, id, src}; }


    // task
    class task: public message
    {
    private:

        task_environment* env;

    protected:

        template<class Type, class... InfoTypes>
        mes_id<Type> create_message(InfoTypes*... info) const;
        template<class Type, class ParentType, class... InfoTypes>
        mes_id<Type> create_message_child(mes_id<ParentType> source, InfoTypes*... info) const;

        template<class Type, class... InfoTypes>
        mes_id<Type> add_message(Type* m, InfoTypes*... info) const;
        template<class Type, class ParentType, class... InfoTypes>
        mes_id<Type> add_message_child(Type* m, mes_id<ParentType> source, InfoTypes*... info) const;

        template<class Type, class... InfoTypes, class... ArgTypes>
        new_task_id<Type> create_task(std::tuple<mes_id<ArgTypes>...> args, InfoTypes*... info) const;
        template<class Type, class... InfoTypes, class... ArgTypes>
        new_task_id<Type> create_child_task(std::tuple<mes_id<ArgTypes>...> args, InfoTypes*... info) const;

        template<class Type, class... ArgTypes>
        new_task_id<Type> create_task(mes_id<ArgTypes>... args) const;
        template<class Type, class... ArgTypes>
        new_task_id<Type> create_child_task(mes_id<ArgTypes>... args) const;

        template<class Type, class... ArgTypes>
        new_task_id<Type> add_task(mes_id<Type> t, mes_id<ArgTypes>... args) const;
        template<class Type, class... ArgTypes>
        new_task_id<Type> add_child_task(mes_id<Type> t, mes_id<ArgTypes>... args) const;

        template<class Type, class... ArgTypes>
        new_task_id<Type> add_task(Type* t, mes_id<ArgTypes>... args) const;
        template<class Type, class... ArgTypes>
        new_task_id<Type> add_child_task(Type* t, mes_id<ArgTypes>... args) const;

        void add_dependence(local_task_id parent, local_task_id child) const;

        template<size_t Index, class Type>
        mes_id<Type> arg_id() const;

        template<class Type>
        new_task_id<Type> this_task_id() const;

        size_t get_workers_count() const;
        size_t get_processes_count() const;
        size_t get_threads_count() const;

    public:

        task();
        virtual ~task();

        virtual void send(const sender& se) const override;
        virtual void recv(const receiver& re) override;

        void set_environment(task_environment* e);
    };

    template<size_t Index, class Type>
    mes_id<Type> task::arg_id() const
    { return env->arg_id(Index); }

    template<class Type, class... InfoTypes>
    mes_id<Type> task::create_message(InfoTypes*... info) const
    {
        std::vector<message*> v;
        tuple_processors<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(v, std::make_tuple(info...));
        return env->create_message_init(message_init_factory::get_type<Type, InfoTypes...>(), v);
    }

    template<class Type, class ParentType, class... InfoTypes>
    mes_id<Type> task::create_message_child(mes_id<ParentType> source, InfoTypes*... info) const
    {
        std::vector<message*> v;
        tuple_processors<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(v, std::make_tuple(info...));
        return env->create_message_child(message_child_factory::get_type<Type, ParentType, InfoTypes...>(), source, v);
    }

    template<class Type, class... InfoTypes>
    mes_id<Type> task::add_message(Type* m, InfoTypes*... info) const
    {
        std::vector<message*> v;
        tuple_processors<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(v, std::make_tuple(info...));
        return env->add_message_init(message_init_factory::get_type<Type, InfoTypes...>(), transform_to_message(m), v);
    }

    template<class Type, class ParentType, class... InfoTypes>
    mes_id<Type> task::add_message_child(Type* m, mes_id<ParentType> source, InfoTypes*... info) const
    {
        std::vector<message*> v;
        tuple_processors<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(v, std::make_tuple(info...));
        return env->add_message_child(message_child_factory::get_type<Type, ParentType, InfoTypes...>(), transform_to_message(m), source, v);
    }

    template<class Type>
    new_task_id<Type> task::this_task_id() const
    { return env->get_this_task_id(); }

    template<typename Type>
    std::enable_if_t<std::is_const<Type>::value> choose_vector_to_push(mes_id<Type> m, std::vector<local_message_id>& v, std::vector<local_message_id>& cv)
    { cv.push_back(m); }

    template<typename Type>
    std::enable_if_t<!std::is_const<Type>::value> choose_vector_to_push(mes_id<Type> m, std::vector<local_message_id>& v, std::vector<local_message_id>& cv)
    { v.push_back(m); }

    template<size_t Pos, typename... Args>
    struct ids_to_two_vectors_impl
    {
        static constexpr size_t Index = sizeof...(Args) - Pos;
        using ArgType = std::tuple_element_t<Index, std::tuple<Args...>>;

        static void perform(std::vector<local_message_id>& v, std::vector<local_message_id>& cv, const std::tuple<mes_id<Args>...>& t)
        {
            choose_vector_to_push(std::get<Index>(t), v, cv);
            ids_to_two_vectors_impl<Pos - 1, Args...>::perform(v, cv, t);
        }
    };

    template<typename... Args>
    struct ids_to_two_vectors_impl<0, Args...>
    {
        static void perform(std::vector<local_message_id>& v, std::vector<local_message_id>& cv, const std::tuple<mes_id<Args>...>& t)
        { }
    };

    template<typename... Args>
    void ids_to_two_vectors(std::vector<local_message_id>& v, std::vector<local_message_id>& cv, const std::tuple<mes_id<Args>...>& t)
    { ids_to_two_vectors_impl<sizeof...(Args), Args...>::perform(v, cv, t); }

    template<typename Type>
    std::enable_if_t<std::is_const<Type>::value, bool> mark_const_as_bool()
    { return true; }

    template<typename Type>
    std::enable_if_t<!std::is_const<Type>::value, bool> mark_const_as_bool()
    { return false; }

    template<size_t Pos, typename... Args>
    struct get_const_map_impl
    {
        static constexpr size_t Index = sizeof...(Args) - Pos;
        using ArgType = std::tuple_element_t<Index, std::tuple<Args...>>;

        static void perform(std::vector<bool>& v)
        {
            v.at(Index) = mark_const_as_bool<ArgType>();
            get_const_map_impl<Pos - 1, Args...>::perform(v);
        }
    };

    template<typename... Args>
    struct get_const_map_impl<0, Args...>
    {
        static void perform(std::vector<bool>& v)
        { }
    };

    template<typename... Args>
    std::vector<bool> get_const_map()
    {
        std::vector<bool> v(sizeof...(Args));
        get_const_map_impl<sizeof...(Args), Args...>::perform(v);
        return v;
    }

    class task_factory
    {
    private:

        class performer_base
        {
        public:

            performer_base();
            virtual ~performer_base();

            virtual void perform(task* t, const std::vector<message*>& args, const std::vector<const message*>& c_args) const = 0;
            virtual const std::vector<bool>& const_map() const = 0;

        };

        template<typename Type, typename... ArgTypes>
        struct id
        {
            task_type value;
            id();
        };

        template<typename Type, typename... ArgTypes>
        class performer: public performer_base
        {
        private:

            static id<Type, ArgTypes...> my_type;
            static std::vector<bool> const_map_v;

        public:

            performer();
            ~performer();

            void perform(task* t, const std::vector<message*>& args, const std::vector<const message*>& c_args) const override final;
            const std::vector<bool>& const_map() const override final;
            static task_type get_type();
        };

        static std::vector<std::unique_ptr<performer_base>>& task_vec();

        template<typename Type, typename... ArgTypes>
        static task_type add();


    public:

        task_factory() = delete;

        static task* get(message_type id, const std::vector<message*>& info);
        static std::vector<message*> get_info(message_type id);

        static void perform(task_type id, task* t, const std::vector<message*>& args, const std::vector<const message*>& c_args);

        static const std::vector<bool>& const_map(task_type id);

        template<typename Type, typename... ArgTypes>
        static task_type get_type();

    };

    template<typename Type, typename... ArgTypes>
    task_factory::id<Type, ArgTypes...>::id(): value(task_factory::add<Type, ArgTypes...>())
    { }

    template<typename Type, typename... ArgTypes>
    task_factory::performer<Type, ArgTypes...>::performer(): performer_base()
    { }

    template<typename Type, typename... ArgTypes>
    task_factory::performer<Type, ArgTypes...>::~performer()
    { }

    template<typename Type, typename... ArgTypes>
    task_factory::id<Type, ArgTypes...> task_factory::performer<Type, ArgTypes...>::my_type;

    template<typename Type, typename... ArgTypes>
    task_type task_factory::performer<Type, ArgTypes...>::get_type()
    { return my_type.value; }

    template<typename Type, typename... ArgTypes>
    void task_factory::performer<Type, ArgTypes...>::perform(task* t, const std::vector<message*>& args, const std::vector<const message*>& c_args) const
    {
        std::tuple<empty_ref_wrapper<ArgTypes>...> tp;
        size_t ind1 = 0, ind2 = 0;
        tuple_processors<sizeof...(ArgTypes), ArgTypes...>::two_vectors_to_ref_tuple(args, c_args, ind1, ind2, tp);
        apply([&t](empty_ref_wrapper<ArgTypes>... args2)->void
        {
            (*dynamic_cast<Type*>(t))(static_cast<ArgTypes&>(args2)...);
        }, tp);
    }

    template<typename Type, typename... ArgTypes>
    std::vector<bool> task_factory::performer<Type, ArgTypes...>::const_map_v(get_const_map<ArgTypes...>());

    template<typename Type, typename... ArgTypes>
    const std::vector<bool>& task_factory::performer<Type, ArgTypes...>::const_map() const
    { return const_map_v; }

    template<typename Type, typename... ArgTypes>
    task_type task_factory::add()
    {
        std::unique_ptr<performer_base> p(new performer<Type, ArgTypes...>());
        task_vec().push_back(std::move(p));
        return task_vec().size() - 1;
    }

    template<typename Type, typename... ArgTypes>
    task_type task_factory::get_type()
    { return performer<Type, ArgTypes...>::get_type(); }


    // task
    template<class Type, class... ArgTypes>
    new_task_id<Type> task::add_task(mes_id<Type> t, mes_id<ArgTypes>... args) const
    {
        std::vector<local_message_id> data, const_data;
        ids_to_two_vectors(data, const_data, std::make_tuple(args...));
        return env->add_task(task_factory::get_type<Type, ArgTypes...>(), t, data, const_data);
    }

    template<class Type, class... ArgTypes>
    new_task_id<Type> task::add_child_task(mes_id<Type> t, mes_id<ArgTypes>... args) const
    {
        std::vector<local_message_id> data, const_data;
        ids_to_two_vectors(data, const_data, std::make_tuple(args...));
        return env->add_child_task(task_factory::get_type<Type, ArgTypes...>(), t, data, const_data);
    }

    template<class Type, class... ArgTypes>
    new_task_id<Type> task::add_task(Type* t, mes_id<ArgTypes>... args) const
    { return add_task(add_message(t), args...); }

    template<class Type, class... ArgTypes>
    new_task_id<Type> task::add_child_task(Type* t, mes_id<ArgTypes>... args) const
    { return add_child_task(add_message(t), args...); }

    template<class Type, class... InfoTypes, class... ArgTypes>
    new_task_id<Type> task::create_task(std::tuple<mes_id<ArgTypes>...> args, InfoTypes*... info) const
    {
        std::vector<message*> v;
        tuple_processors<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(v, std::make_tuple(info...));
        std::vector<local_message_id> data, const_data;
        ids_to_two_vectors(data, const_data, args);
        return env->create_task(message_init_factory::get_type<Type, InfoTypes...>(), task_factory::get_type<Type, ArgTypes...>(), data, const_data, v);
    }

    template<class Type, class... InfoTypes, class... ArgTypes>
    new_task_id<Type> task::create_child_task(std::tuple<mes_id<ArgTypes>...> args, InfoTypes*... info) const
    {
        std::vector<message*> v;
        tuple_processors<sizeof...(InfoTypes), InfoTypes...>::create_vector_from_pointers(v, std::make_tuple(info...));
        std::vector<local_message_id> data, const_data;
        ids_to_two_vectors(data, const_data, args);
        return env->create_child_task(message_init_factory::get_type<Type, InfoTypes...>(), task_factory::get_type<Type, ArgTypes...>(), data, const_data, v);
    }

    template<class Type, class... ArgTypes>
    new_task_id<Type> task::create_task(mes_id<ArgTypes>... args) const
    {
        std::vector<message*> v;
        std::vector<local_message_id> data, const_data;
        ids_to_two_vectors(data, const_data, std::make_tuple(args...));
        return env->create_task(message_init_factory::get_type<Type>(), task_factory::get_type<Type, ArgTypes...>(), data, const_data, v);
    }

    template<class Type, class... ArgTypes>
    new_task_id<Type> task::create_child_task(mes_id<ArgTypes>... args) const
    {
        std::vector<message*> v;
        std::vector<local_message_id> data, const_data;
        ids_to_two_vectors(data, const_data, std::make_tuple(args...));
        return env->create_child_task(message_init_factory::get_type<Type>(), task_factory::get_type<Type, ArgTypes...>(), data, const_data, v);
    }

}

#endif // __BASIC_TASK_H__
