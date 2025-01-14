// PyRowRef.h --
// $Id$
// This is part of MetaKit, see http://www.equi4.com/metakit.html
// Copyright (C) 1999-2004 Gordon McMillan and Jean-Claude Wippler.
//
//  RowRef class header

#if !defined INCLUDE_PYROWREF_H
#define INCLUDE_PYROWREF_H

#include <mk4.h>
#include "PyHead.h"
#include <PWOSequence.h>
#include "PyView.h"
#include "PyPropRef.h"

#define PyRowRef_Check(ob) ((ob)->ob_type == &PyRowRef_Type)
#define PyRORowRef_Check(ob) ((ob)->ob_type == &PyRORowRef_Type)
#define PyGenericRowRef_Check(ob) (PyRowRef_Check(ob) || PyRORowRef_Check(ob))

extern PyTypeObject PyRowRef_Type;
extern PyTypeObject PyRORowRef_Type;

class PyRowRef: public PyHead, public c4_RowRef {
  public:
    //PyRowRef();
    PyRowRef(const c4_RowRef &o, int immutable = 0);
    //PyRowRef(c4_Row row);
    ~PyRowRef() {
        c4_Cursor c = &(*(c4_RowRef*)this);
        c._seq->DecRef();
    }
    PyPropRef *getProperty(const char *nm) {
        c4_View cntr = Container();
        int ndx = cntr.FindPropIndexByName(nm);
        if (ndx >  - 1) {
            return new PyPropRef(cntr.NthProperty(ndx));
        }
        return 0;
    };

    PyObject *getPropertyValue(const char *nm) {
        PyPropRef *prop = getProperty(nm);
        if (prop) {
            PyObject *result = asPython(*prop);
            Py_DECREF(prop);
            return result;
        }
        return 0;
    };

    static void setFromPython(const c4_RowRef &row, const c4_Property &prop,
      PyObject *val);
    static void setDefault(const c4_RowRef &row, const c4_Property &prop);
    PyObject *asPython(const c4_Property &prop);
};

#endif
