#include <cstring>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <random>
#include "parallel.h"
using namespace apl;

bool inf_runtime = false;
bool output = false;
int average_task_count = 10;
int average_message_count = 10;

class random_task: public task
{
public:
    void operator()(int count, int& m_count, long long gen)
    {
        if (output)
            std::cout << count << ' ' << m_count << ' ' << gen << std::endl;
        ++gen;
        std::mt19937 mt(static_cast<unsigned>(gen));

        for (int i = 0; i < m_count; ++i)
        {
            std::uniform_int_distribution<int> uid(0, 1);
            int inst = uid(mt);
            switch (inst)
            {
            case 0:
                create_message<size_t>();
                break;
            case 1:
                add_message(new size_t());
                break;
            default:
                break;
            }
        }

        for (int i = 0; i < count; ++i)
        {
            int new_count = 0;
            if (inf_runtime)
            {
                if (count > average_task_count)
                {
                    std::uniform_int_distribution<int> uid(0, count / 2);
                    new_count = uid(mt);
                }
                else
                {
                    std::uniform_int_distribution<int> uid(count + 1, (count == 0) ? 1: count * 2);
                    new_count = uid(mt);
                }
            }
            else
            {
                std::uniform_int_distribution<int> uid(0, count - 1);
                new_count = uid(mt);
            }

            int new_m_count = 0;
            if (m_count > average_message_count)
            {
                std::uniform_int_distribution<int> uid(0, m_count / 2);
                new_m_count = uid(mt);
            }
            else
            {
                std::uniform_int_distribution<int> uid(m_count + 1, (m_count == 0) ? 1: m_count * 2);
                new_m_count = uid(mt);
            }
            ++gen;
            std::uniform_int_distribution<int> uid(0, 3);
            int inst = uid(mt);
            switch (inst)
            {
            case 0:
                create_child_task<random_task>(add_message(new int(new_count)).as_const(),
                    add_message(new int(new_m_count)),
                    add_message(new long long(gen)).as_const());
                break;
            case 1:
                create_task<random_task>(add_message(new int(new_count)).as_const(),
                    add_message(new int(new_m_count)),
                    add_message(new long long(gen)).as_const());
                break;
            case 2:
                add_child_task(new random_task(), add_message(new int(new_count)).as_const(),
                    add_message(new int(new_m_count)),
                    add_message(new long long(gen)).as_const());
                break;
            case 3:
                add_task(new random_task(), add_message(new int(new_count)).as_const(),
                    add_message(new int(new_m_count)),
                    add_message(new long long(gen)).as_const());
                break;
            default:
                break;
            }
        }
    }
};

int main(int argc, char** argv)
{
    parallel_engine pe(&argc, &argv);

    for (int i = 1; i < argc; ++i)
    {
        if (strcmp(argv[i], "-t") == 0)
        {
            average_task_count = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "-m") == 0)
        {
            average_message_count = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "-out") == 0)
        {
            output = true;
        }
        else if (strcmp(argv[i], "-inf") == 0)
        {
            inf_runtime = true;
        }
    }

    parallelizer pz;

    random_task it;
    pz.execution(&it, std::make_tuple(), const_cast<const int*>(new int(average_task_count)), new int(average_message_count),
        const_cast<const long long*>(new long long(time(0))));
}
