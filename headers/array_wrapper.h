#ifndef __ARRAY_WRAPPER_H__
#define __ARRAY_WRAPPER_H__

#include "message.h"
#include "message_factory.h"

namespace apl
{

    template<typename Type>
    class array_wrapper: public message_wrapper<Type*>
    {
    private:

        size_t count;

    public:

        array_wrapper(size_t sz);
        array_wrapper(Type* p, size_t sz);
        array_wrapper(const array_wrapper& aw, size_t new_size, size_t offset);
        array_wrapper(size_t sz, size_t offset);

        void include(const array_wrapper& child, size_t sz, size_t offset);

        ~array_wrapper();

        Type& operator[](size_t n);
        const Type& operator[](size_t n) const;

        Type* data();
        const Type* data() const;

        size_t size() const;

        void send(const sender& se) const;

        void recv(const receiver& re);
    };

    template<typename Type>
    array_wrapper<Type>::array_wrapper(size_t sz): message_wrapper<Type*>(new Type[sz], true), count(sz)
    { }

    template<typename Type>
    array_wrapper<Type>::array_wrapper(Type* p, size_t sz): message_wrapper<Type*>(p, true), count(sz)
    { }

    template<typename Type>
    array_wrapper<Type>::array_wrapper(const array_wrapper& aw, size_t new_size, size_t offset): message_wrapper<Type*>(aw.ptr + offset, false), count(new_size)
    { }

    template<typename Type>
    array_wrapper<Type>::array_wrapper(size_t sz, size_t offset): message_wrapper<Type*>(new Type[sz], true), count(sz)
    { }

    template<typename Type>
    std::enable_if_t<std::is_array<Type>::value && (std::rank<Type>::value == 1)> include(Type& parent, const Type& child, size_t sz, size_t offset)
    {
        
    }

    template<typename Type>
    void array_wrapper<Type>::include(const array_wrapper& child, size_t sz, size_t offset)
    {
        if (child.allocated)
        {
            Type* q = ptr + offset;
            for (size_t i = 0; i < child.size(); ++i)
                q[i] = child[i];
        }
    }

    template<typename Type>
    array_wrapper<Type>::~array_wrapper()
    {
        if (allocated)
            delete[] ptr;
        ptr = nullptr;
    }

    template<typename Type>
    Type& array_wrapper<Type>::operator[](size_t n)
    { return ptr[n]; }

    template<typename Type>
    const Type& array_wrapper<Type>::operator[](size_t n) const
    { return ptr[n]; }

    template<typename Type>
    Type* array_wrapper<Type>::data()
    { return ptr; }

    template<typename Type>
    const Type* array_wrapper<Type>::data() const
    { return ptr; }

    template<typename Type>
    size_t array_wrapper<Type>::size() const
    { return count; }

    template<typename Type>
    void array_wrapper<Type>::send(const sender& se) const
    { se.isend(ptr, count); }

    template<typename Type>
    void array_wrapper<Type>::recv(const receiver& re)
    { re.irecv(ptr, count); }

    template<typename Type>
    array_wrapper<Type>* make_array(Type* p, size_t sz)
    { return new array_wrapper<Type>(p, sz); }

}

#endif // __ARRAY_WRAPPER_H__
