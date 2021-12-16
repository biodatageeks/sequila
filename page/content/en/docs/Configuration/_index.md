
---
title: "Configuration"
linkTitle: "Configuration"
weight: 14
description: >
    Glossary of SeQuiLa's parameters and configuration options
---

## How to set parameters
{{< tabpane >}}
{{< tab header="SQL" lang="sql" >}}
SET spark.biodatageeks.readAligment.method=disq;
{{< /tab >}}
{{< tab header="Python" lang="python">}}
from pysequila import SequilaSession
ss = SequilaSession \
.builder \
.getOrCreate()
ss.sql("SET spark.biodatageeks.readAligment.method=disq")
{{< /tab >}}
{{< /tabpane >}}




