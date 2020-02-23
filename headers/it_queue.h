#ifndef __IT_QUEUE__
#define __IT_QUEUE__

#include <algorithm>

namespace auto_parallel
{

    template<typename type>
    class it_queue
    {
    private:

        static size_t max_size;
        type* _value;
        size_t first, last, _size, _capacity;
        bool full;

    public:

        it_queue();
        ~it_queue();

        const type& front();
        void push(const type& val);
        void pop();

        type& operator[](size_t n);
        const type& operator[](size_t n) const;

        bool empty();

        size_t size();
        size_t capacity();

    };

    template<typename type>
    size_t it_queue<type>::max_size = 1073741823u;

    template<typename type>
    it_queue<type>::it_queue()
    {
        first = last = 0u;
        full = true;
        _size = _capacity = 0u;
        _value = nullptr;
    }

    template<typename type>
    it_queue<type>::~it_queue()
    { delete[] _value; }

    template<typename type>
    const type& it_queue<type>::front()
    { return _value[first]; }

    template<typename type>
    void it_queue<type>::push(const type& val)
    {
        if (full)
        {
            if (_value == nullptr)
            {
                _value = new type[1u];
                _capacity = 1u;
            }
            else
            {
                type* p = new type[std::min((_capacity << 1),max_size)];
                for (size_t i = 0u, j = first; i < _capacity; ++i,j = (j + 1u) % _capacity)
                    p[i] = _value[j];
                first = 0u;
                last = _capacity;
                _capacity = std::min((_capacity << 1),max_size);
                delete[] _value;
                _value = p;
                full = false;
            }
        }
        _value[last] = val;
        last = (last + 1u) % _capacity;
        ++_size;
        if (first == last)
            full = true;
    }

    template<typename type>
    void it_queue<type>::pop()
    {
        first = (first + 1u) % _capacity;
        --_size;
        full = false;
    }

    template<typename type>
    type& it_queue<type>::operator[](size_t n)
    { return _value[(first + n) % _capacity]; }

    template<typename type>
    const type& it_queue<type>::operator[](size_t n) const
    { return _value[(first + n) % _capacity]; }

    template<typename type>
    bool it_queue<type>::empty()
    { return (first == last) && (!full); }

    template<typename type>
    size_t it_queue<type>::size()
    { return _size; }

    template<typename type>
    size_t it_queue<type>::capacity()
    { return _capacity; }

}
#endif // __IT_QUEUE__
