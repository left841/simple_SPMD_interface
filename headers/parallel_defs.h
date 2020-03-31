#ifndef __PARALLEL_DEFS_H__
#define __PARALLEL_DEFS_H__

#include "mpi.h"
#include <limits>

namespace auto_parallel
{

    typedef size_t task_id;
    typedef size_t message_id;
    typedef size_t task_type;
    typedef size_t message_type;
    typedef int process;

    const task_id TASK_ID_UNDEFINED = std::numeric_limits<task_id>::max();
    const message_id MESSAGE_ID_UNDEFINED = std::numeric_limits<message_id>::max();
    const task_type TASK_TYPE_UNDEFINED = std::numeric_limits<task_type>::max();
    const message_type MESSAGE_TYPE_UNDEFINED = std::numeric_limits<message_type>::max();

}

#endif // __PARALLEL_DEFS_H__
