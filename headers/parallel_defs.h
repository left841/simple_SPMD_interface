#ifndef __PARALLEL_DEFS_H__
#define __PARALLEL_DEFS_H__

#include "mpi.h"
#include <limits>

namespace apl
{

    typedef int process;

    typedef size_t message_type;
    typedef size_t perform_type;

    struct task_type
    {
        message_type mt;
        perform_type pt;
    };

    constexpr const message_type MESSAGE_TYPE_UNDEFINED = std::numeric_limits<message_type>::max();
    constexpr const perform_type PERFORM_TYPE_UNDEFINED = std::numeric_limits<perform_type>::max();
    constexpr const task_type TASK_TYPE_UNDEFINED = {MESSAGE_TYPE_UNDEFINED, PERFORM_TYPE_UNDEFINED};

    enum class MESSAGE_SOURCE: size_t
    {
        GLOBAL, TASK_ARG, TASK_ARG_C,
        REFERENCE,
        INIT, CHILD,
        INIT_A, CHILD_A
    };

    enum class TASK_SOURCE: size_t
    {
        GLOBAL, INIT, REFERENCE,
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

        mes_id(local_message_id id);
        mes_id(const mes_id<Type>& id);

        mes_id<const Type> as_const();
        operator local_message_id();
    };

    template<typename Type>
    mes_id<Type>::mes_id(local_message_id id): id(id.id), src(id.src)
    { }

    template<typename Type>
    mes_id<Type>::mes_id(const mes_id<Type>& id) : id(id.id), src(id.src)
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

    enum class TAG
    {
        UNDEFINED = std::numeric_limits<int>::max(),
        ANY = MPI_ANY_TAG,
        MAIN = 0,
        SIZE = 1
    };

}

#endif // __PARALLEL_DEFS_H__
