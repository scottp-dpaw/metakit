// PyStorage.h --
// $Id$
// This is part of MetaKit, see http://www.equi4.com/metakit.html
// Copyright (C) 1999-2004 Gordon McMillan and Jean-Claude Wippler.
//
//  Storage class header

#if !defined INCLUDE_PYSTORAGE_H
#define INCLUDE_PYSTORAGE_H

#include <mk4.h>
#include "PyHead.h"

extern PyTypeObject PyStorage_Type;
class SiasStrategy;

#define PyStorage_Check(v) ((v)->ob_type==&PyStorage_Type)

class PyStorage: public PyHead, public c4_Storage {
  public:
    PyStorage(): PyHead(PyStorage_Type){}
    PyStorage(c4_Strategy &strategy_, bool owned_ = false, int mode_ = 1):
      PyHead(PyStorage_Type), c4_Storage(strategy_, owned_, mode_){}
    PyStorage(const char *fnm, int mode): PyHead(PyStorage_Type), c4_Storage(fnm,
      mode){}
    PyStorage(const c4_Storage &storage_): PyHead(PyStorage_Type), c4_Storage
      (storage_){}
    //  PyStorage(const char *fnm, const char *descr) 
    //    : PyHead(PyStorage_Type), c4_Storage(fnm, descr) { }
    ~PyStorage(){}
};

#endif
