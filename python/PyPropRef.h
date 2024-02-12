// PyPropRef.h --
// $Id$
// This is part of MetaKit, see http://www.equi4.com/metakit.html
// Copyright (C) 1999-2004 Gordon McMillan and Jean-Claude Wippler.
//
//  Property class header

#if !defined INCLUDE_PYPROPERTY_H
#define INCLUDE_PYPROPERTY_H

#include <mk4.h>
#include "PyHead.h"

#define PyPropRef_Check(ob) ((ob)->ob_type == &PyPropRef_Type)

extern PyTypeObject PyPropRef_Type;

class PyPropRef: public PyHead, public c4_Property {
  public:
    //PyPropRef();
    PyPropRef(const c4_Property &o): PyHead(PyPropRef_Type), c4_Property(o){}
    PyPropRef(char t, const char *n): PyHead(PyPropRef_Type), c4_Property(t, n)
      {}
    ~PyPropRef(){}
};

PyObject *PyPropRef_new(PyObject *o, PyObject *_args);

#endif
