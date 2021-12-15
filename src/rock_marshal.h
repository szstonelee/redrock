#ifndef __ROCK_MARSHAL_H
#define __ROCK_MARSHAL_H

#include "server.h"

sds marshal_object(const robj* o);
robj* unmarshal_object(const sds v);
int debug_check_type(const sds recover_val, const robj *shared_obj);
robj* get_match_rock_value(const robj *o);

#endif