#ifndef __MESSAGE_FACTORY_H__
#define __MESSAGE_FACTORY_H__

#include "message.h"
#include <tuple>
#include <memory>

namespace apl
{

    template<size_t... Indexes>
    struct index_sequence
    { };

    template<size_t Pos, size_t... Indexes>
    struct make_index_sequence: make_index_sequence<Pos - 1, Pos - 1, Indexes...>
    { };

    template<size_t... Indexes>
    struct make_index_sequence<0, Indexes...>
    {
        typedef index_sequence<Indexes...> type;
    };

    template<size_t... Indexes, typename Func, typename... Args>
    auto apply_impl(index_sequence<Indexes...>, Func&& f, const std::tuple<Args...>& args)
    { return f(std::get<Indexes>(args)...); }

    template<size_t... Indexes, typename Func, typename... Args>
    auto apply_impl(index_sequence<Indexes...>, Func&& f, std::tuple<Args...>& args)
    { return f(std::get<Indexes>(args)...); }

    template<typename Func, typename... Args>
    auto apply(Func&& f, const std::tuple<Args...>& args)
    { return apply_impl(typename make_index_sequence<sizeof...(Args)>::type(), std::forward<Func>(f), args); }

    template<typename Func, typename... Args>
    auto apply(Func&& f, std::tuple<Args...>& args)
    { return apply_impl(typename make_index_sequence<sizeof...(Args)>::type(), std::forward<Func>(f), args); }

    template<typename Type>
    class empty_ref_wrapper
    {
    private:
        Type* p;

    public:
        empty_ref_wrapper();
        empty_ref_wrapper(const Type& src);
        empty_ref_wrapper<Type>& operator=(const empty_ref_wrapper<Type>& src);

        operator Type&();
        operator const Type&() const;

        void set(const Type& src);

    };

    template<typename Type>
    empty_ref_wrapper<Type>::empty_ref_wrapper(): p(nullptr)
    { }

    template<typename Type>
    empty_ref_wrapper<Type>::empty_ref_wrapper(const Type& src): p(std::addressof(const_cast<Type&>(src)))
    { }

    template<typename Type>
    empty_ref_wrapper<Type>& empty_ref_wrapper<Type>::operator=(const empty_ref_wrapper<Type>& src)
    {
        p = src.p;
        return *this;
    }

    template<typename Type>
    empty_ref_wrapper<Type>::operator Type&()
    { return *p; }

    template<typename Type>
    empty_ref_wrapper<Type>::operator const Type&() const
    { return *p; }

    template<typename Type>
    void empty_ref_wrapper<Type>::set(const Type& src)
    { p = std::addressof(const_cast<Type&>(src)); }


    template<typename Type>
    std::enable_if_t<std::is_base_of<message, Type>::value, message*> transform_to_message(Type* p)
    { return p; }

    template<typename Type>
    std::enable_if_t<!std::is_base_of<message, Type>::value, message*> transform_to_message(Type* p)
    { return new message_wrapper<Type>(p); }

    template<typename Type>
    std::enable_if_t<std::is_base_of<message, Type>::value, Type*> transform_from_message(message* p)
    { return dynamic_cast<Type*>(p); }

    template<typename Type>
    std::enable_if_t<!std::is_base_of<message, Type>::value, Type*> transform_from_message(message* p)
    { return dynamic_cast<message_wrapper<Type>*>(p)->get(); }

    template<typename Type>
    std::enable_if_t<std::is_base_of<message, Type>::value, const Type*> transform_from_message(const message* p)
    { return dynamic_cast<const Type*>(p); }

    template<typename Type>
    std::enable_if_t<!std::is_base_of<message, Type>::value, const Type*> transform_from_message(const message* p)
    { return dynamic_cast<const message_wrapper<Type>*>(p)->get(); }

    template<typename Type>
    std::enable_if_t<std::is_const<Type>::value, Type*> choose_vector(const std::vector<message*>& v, const std::vector<const message*>& cv, size_t& v_pos, size_t& cv_pos)
    {
        cv_pos += 1;
        return transform_from_message<typename std::remove_const<Type>::type>(cv.at(cv_pos - 1));
    }

    template<typename Type>
    std::enable_if_t<!std::is_const<Type>::value, Type*> choose_vector(const std::vector<message*>& v, const std::vector<const message*>& cv, size_t& v_pos, size_t& cv_pos)
    {
        v_pos += 1;
        return transform_from_message<Type>(v.at(v_pos - 1));
    }

    template<typename Type>
    std::enable_if_t<std::is_const<Type>::value> choose_vector_to_push(mes_id<Type> m, std::vector<local_message_id>& v, std::vector<local_message_id>& cv)
    { cv.push_back(m); }

    template<typename Type>
    std::enable_if_t<!std::is_const<Type>::value> choose_vector_to_push(mes_id<Type> m, std::vector<local_message_id>& v, std::vector<local_message_id>& cv)
    { v.push_back(m); }

    template<typename Type>
    std::enable_if_t<std::is_const<Type>::value, bool> mark_const_as_bool()
    { return true; }

    template<typename Type>
    std::enable_if_t<!std::is_const<Type>::value, bool> mark_const_as_bool()
    { return false; }

    template<size_t Pos, typename... Args>
    class tuple_processers
    {
    private:
        typedef std::tuple_element_t<sizeof...(Args) - Pos, std::tuple<Args...>> arg_type;

    public:
        static void create_vector_of_args(std::vector<message*>& v)
        {
            arg_type* t = new arg_type();
            v.push_back(transform_to_message(t));
            tuple_processers<Pos - 1, Args...>::create_vector_of_args(v);
        }

        static void vector_to_ref_tuple(const std::vector<message*>& v, std::tuple<empty_ref_wrapper<Args>...>& t)
        {
            std::get<sizeof...(Args) - Pos>(t).set(*transform_from_message<arg_type>(v.at(sizeof...(Args) - Pos)));
            tuple_processers<Pos - 1, Args...>::vector_to_ref_tuple(v, t);
        }

        static void two_vectors_to_ref_tuple(const std::vector<message*>& v, const std::vector<const message*>& cv, size_t& v_pos, size_t& cv_pos, std::tuple<empty_ref_wrapper<Args>...>& t)
        {
            std::get<sizeof...(Args) - Pos>(t).set(*choose_vector<arg_type>(v, cv, v_pos, cv_pos));
            tuple_processers<Pos - 1, Args...>::two_vectors_to_ref_tuple(v, cv, v_pos, cv_pos, t);
        }

        static void create_vector_from_pointers(std::vector<message*>& v, const std::tuple<Args*...>& t)
        {
            v.push_back(transform_to_message(std::get<sizeof...(Args) - Pos>(t)));
            tuple_processers<Pos - 1, Args...>::create_vector_from_pointers(v, t);
        }

        static void ids_to_two_vectors(std::vector<local_message_id>& v, std::vector<local_message_id>& cv, const std::tuple<mes_id<Args>...>& t)
        {
            choose_vector_to_push<arg_type>(std::get<sizeof...(Args) - Pos>(t), v, cv);
            tuple_processers<Pos - 1, Args...>::ids_to_two_vectors(v, cv, t);
        }

        static void get_const_map_impl(std::vector<bool>& v)
        {
            v.at(sizeof...(Args) - Pos) = mark_const_as_bool<arg_type>();
            tuple_processers<Pos - 1, Args...>::get_const_map_impl(v);
        }
    };

    template<typename... Args>
    class tuple_processers<0, Args...>
    {
    public:
        static void create_vector_of_args(std::vector<message*>& v)
        { }

        static void vector_to_ref_tuple(const std::vector<message*>& v, std::tuple<empty_ref_wrapper<Args>...>& t)
        { }

        static void two_vectors_to_ref_tuple(const std::vector<message*>& v, const std::vector<const message*>& cv, size_t& v_pos, size_t& cv_pos, std::tuple<empty_ref_wrapper<Args>...>& t)
        { }

        static void create_vector_from_pointers(std::vector<message*>& v, const std::tuple<Args*...>& tp)
        { }

        static void ids_to_two_vectors(std::vector<local_message_id>& v, std::vector<local_message_id>& cv, const std::tuple<mes_id<Args>...>& t)
        { }

        static void get_const_map_impl(std::vector<bool>& v)
        { }
    };

    template<typename... Args>
    std::vector<bool> get_const_map()
    {
        std::vector<bool> v(sizeof...(Args));
        tuple_processers<sizeof...(Args), Args...>::get_const_map_impl(v);
        return v;
    }

    enum class MESSAGE_FACTORY_TYPE: size_t
    {
        UNDEFINED, INIT, CHILD
    };

    // init
    class message_init_factory
    {
    private:

        class creator_base
        {
        public:

            creator_base();
            virtual ~creator_base();

            virtual message* get_message(const std::vector<message*>& info) = 0;
            virtual std::vector<message*> get_info() = 0;

        };

        template<typename Type, typename... InfoTypes>
        struct id
        {
            message_type value;
            id();
        };

        template<typename Type, typename... InfoTypes>
        class creator: public creator_base
        {
        private:

            static id<Type, InfoTypes...> my_type;

        public:

            creator();
            ~creator();

            message* get_message(const std::vector<message*>& info);
            std::vector<message*> get_info();

            static message_type get_type();

            friend class message_init_factory;
        };

        message_init_factory() = delete;
        static std::vector<std::unique_ptr<creator_base>>& message_vec();

        template<typename Type, typename... InfoTypes>
        static message_type add();

    public:

        static message* get(message_type id, const std::vector<message*>& info);
        static std::vector<message*> get_info(message_type id);

        template<typename Type, typename... InfoTypes>
        static message_type get_type();
    };

    template<typename Type, typename... InfoTypes>
    message_init_factory::id<Type, InfoTypes...>::id(): value(message_init_factory::add<Type, InfoTypes...>())
    { }

    template<typename Type, typename... InfoTypes>
    message_init_factory::creator<Type, InfoTypes...>::creator()
    { }

    template<typename Type, typename... InfoTypes>
    message_init_factory::creator<Type, InfoTypes...>::~creator()
    { }

    template<typename Type, typename... InfoTypes>
    message* message_init_factory::creator<Type, InfoTypes...>::get_message(const std::vector<message*>& info)
    {
        message* p;
        std::tuple<empty_ref_wrapper<InfoTypes>...> tp;
        tuple_processers<sizeof...(InfoTypes), InfoTypes...>::vector_to_ref_tuple(info, tp);
        apply([&p](empty_ref_wrapper<InfoTypes>... args)->void
        {
            p = transform_to_message(new Type(static_cast<InfoTypes>(args)...));
        }, tp);
        return p;
    }

    template<typename Type, typename... InfoTypes>
    std::vector<message*> message_init_factory::creator<Type, InfoTypes...>::get_info()
    {
        std::vector<message*> v;
        tuple_processers<sizeof...(InfoTypes), InfoTypes...>::create_vector_of_args(v);
        return v;
    }

    template<typename Type, typename... InfoTypes>
    message_init_factory::id<Type, InfoTypes...> message_init_factory::creator<Type, InfoTypes...>::my_type;

    template<typename Type, typename... InfoTypes>
    message_type message_init_factory::creator<Type, InfoTypes...>::get_type()
    { return my_type.value; }

    template<typename Type, typename... InfoTypes>
    message_type message_init_factory::add()
    {
        std::unique_ptr<creator_base> p(new creator<Type, InfoTypes...>());
        message_vec().push_back(std::move(p));
        return message_vec().size() - 1;
    }

    template<typename Type, typename... InfoTypes>
    message_type message_init_factory::get_type()
    { return creator<Type, InfoTypes...>::get_type(); }

    // child
    class message_child_factory
    {
    private:

        class creator_base
        {
        public:

            creator_base();
            virtual ~creator_base();

            virtual message* get_message(const message* parent, const std::vector<message*>& info) = 0;
            virtual message* get_message(const std::vector<message*>& info) = 0;
            virtual std::vector<message*> get_info() = 0;
            virtual void include(message* parent, const message* child, const std::vector<message*>& info) = 0;

        };

        template<typename Type, typename ParentType, typename... InfoTypes>
        struct id
        {
            message_type value;
            id();
        };

        template<typename Type, typename ParentType, typename... InfoTypes>
        class creator: public creator_base
        {
        private:

            static id<Type, ParentType, InfoTypes...> my_type;

        public:

            creator();
            ~creator();

            message* get_message(const message* parent, const std::vector<message*>& info);
            message* get_message(const std::vector<message*>& info);
            std::vector<message*> get_info();
            void include(message* parent, const message* child, const std::vector<message*>& info);

            static message_type get_type();

            friend class message_child_factory;
        };

        message_child_factory() = delete;
        static std::vector<std::unique_ptr<creator_base>>& message_vec();

        template<typename Type, typename ParentType, typename... InfoTypes>
        static message_type add();

    public:

        static message* get(message_type id, const message* parent, const std::vector<message*>& info);
        static message* get(message_type id, const std::vector<message*>& info);
        static std::vector<message*> get_info(message_type id);
        static void include(message_type id, message* parent, const message* child, const std::vector<message*>& info);

        template<typename Type, typename ParentType, typename... InfoTypes>
        static message_type get_type();

    };

    template<typename Type, typename ParentType, typename... InfoTypes>
    message_child_factory::id<Type, ParentType, InfoTypes...>::id(): value(message_child_factory::add<Type, ParentType, InfoTypes...>())
    { }

    template<typename Type, typename ParentType, typename... InfoTypes>
    message_child_factory::creator<Type, ParentType, InfoTypes...>::creator()
    { }

    template<typename Type, typename ParentType, typename... InfoTypes>
    message_child_factory::creator<Type, ParentType, InfoTypes...>::~creator()
    { }

    template<typename Type, typename ParentType, typename... InfoTypes>
    message* message_child_factory::creator<Type, ParentType, InfoTypes...>::get_message(const message* parent, const std::vector<message*>& info)
    {
        message* p;
        std::tuple<empty_ref_wrapper<InfoTypes>...> tp;
        tuple_processers<sizeof...(InfoTypes), InfoTypes...>::vector_to_ref_tuple(info, tp);
        apply([&p, &parent](empty_ref_wrapper<InfoTypes>... args)->void
        {
            p = transform_to_message(new Type(*transform_from_message<ParentType>(parent), static_cast<InfoTypes&>(args)...));
        }, tp);
        return p;
    }

    template<typename Type, typename ParentType, typename... InfoTypes>
    message* message_child_factory::creator<Type, ParentType, InfoTypes...>::get_message(const std::vector<message*>& info)
    {
        message* p;
        std::tuple<empty_ref_wrapper<InfoTypes>...> tp;
        tuple_processers<sizeof...(InfoTypes), InfoTypes...>::vector_to_ref_tuple(info, tp);
        apply([&p](empty_ref_wrapper<InfoTypes>... args)->void
        {
            p = transform_to_message(new Type(static_cast<InfoTypes>(args)...));
        }, tp);
        return p;
    }

    template<typename Type, typename ParentType, typename... InfoTypes>
    std::vector<message*> message_child_factory::creator<Type, ParentType, InfoTypes...>::get_info()
    {
        std::vector<message*> v;
        tuple_processers<sizeof...(InfoTypes), InfoTypes...>::create_vector_of_args(v);
        return v;
    }

    template<typename Type, typename ParentType, typename... InfoTypes>
    void message_child_factory::creator<Type, ParentType, InfoTypes...>::include(message* parent, const message* child, const std::vector<message*>& info)
    {
        std::tuple<empty_ref_wrapper<InfoTypes>...> tp;
        tuple_processers<sizeof...(InfoTypes), InfoTypes...>::vector_to_ref_tuple(info, tp);
        apply([&parent, &child](empty_ref_wrapper<InfoTypes>... args)->void
        {
                (*transform_from_message<ParentType>(parent)).include(*transform_from_message<Type>(child), static_cast<InfoTypes>(args)...);
        }, tp);
    }

    template<typename Type, typename ParentType, typename... InfoTypes>
    message_child_factory::id<Type, ParentType, InfoTypes...> message_child_factory::creator<Type, ParentType, InfoTypes...>::my_type;

    template<typename Type, typename ParentType, typename... InfoTypes>
    message_type message_child_factory::creator<Type, ParentType, InfoTypes...>::get_type()
    { return my_type.value; }

    template<typename Type, typename ParentType, typename... InfoTypes>
    message_type message_child_factory::add()
    {
        std::unique_ptr<creator_base> p(new creator<Type, ParentType, InfoTypes...>());
        message_vec().push_back(std::move(p));
        return message_vec().size() - 1;
    }

    template<typename Type, typename ParentType, typename... InfoTypes>
    message_type message_child_factory::get_type()
    { return creator<Type, ParentType, InfoTypes...>::get_type(); }

}

#endif // __MESSAGE_FACTORY_H__
