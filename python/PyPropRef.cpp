// PyPropRef.cpp --
// $Id$
// This is part of MetaKit, the homepage is http://www.equi4.com/metakit.html
// Copyright (C) 1999-2004 Gordon McMillan and Jean-Claude Wippler.
//
//  Property class implementation

#include "PyPropRef.h"
#include <PWONumber.h>
#include <PWOSequence.h>

static void PyPropRef_dealloc(PyPropRef *o) {
  delete o;
}

PyObject *PyPropRef_repr(PyPropRef *o) {
  return PyUnicode_FromFormat("Property('%c', '%s')", o->Type(), o->Name());
  return 0;
}

static PyObject *PyPropRef_getattro(PyPropRef *o, PyObject *attr) {
  const char *nm = PyUnicode_AsUTF8(attr);
  try {
    if (nm[0] == 'n' && strcmp(nm, "name") == 0) {
      PWOString rslt(o->Name());
      return rslt.disOwn();
    }
    if (nm[0] == 't' && strcmp(nm, "type") == 0) {
      char s = o->Type();
      PWOString rslt(&s, 1);
      return rslt.disOwn();
    }
    if (nm[0] == 'i' && strcmp(nm, "id") == 0) {
      PWONumber rslt(o->GetId());
      return rslt.disOwn();
    }
    return PyObject_GenericGetAttr(o, attr);
  } catch (...) {
    return 0;
  }
}

static int PyPropRef_compare(PyPropRef *o, PyObject *ob) {
  PyPropRef *other;
  int myid, hisid;
  try {
    if (!PyPropRef_Check(ob))
      return  - 1;
    other = (PyPropRef*)ob;
    myid = o->GetId();
    hisid = other->GetId();
    if (myid < hisid)
      return  - 1;
    else if (myid == hisid)
      return 0;
    return 1;
  } catch (...) {
    return  - 1;
  }
}

PyTypeObject PyPropRef_Type = {
  .ob_base = PyObject_HEAD_INIT(&PyType_Type) 
  .tp_name = "PyPropRef", 
  .tp_basicsize = sizeof(PyPropRef),
  .tp_itemsize = 0,
  .tp_dealloc = (destructor)PyPropRef_dealloc,
  .tp_repr = (reprfunc)PyPropRef_repr,
  .tp_getattro = (getattrofunc)PyPropRef_getattro,
};

PyObject *PyPropRef_new(PyObject *o, PyObject *_args) {
  try {
    PWOSequence args(_args);
    PWOString typ(args[0]);
    PWOString nam(args[1]);
    return new PyPropRef(*(const char*)typ, nam);
  } catch (...) {
    return 0;
  }
}
