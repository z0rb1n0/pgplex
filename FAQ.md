### Why did you write this?

It all started after some frustration over the implementation of other pooling/clustering tools. They all did their job well but left out at least ONE important aspect of what we needed, forcing us to layer many of them and have to deal with the complexity/obscurity/overhead drawbacks


---

### Using Python for such a performance critical piece of software? Did you lose your mind?

Nowadays we only use C or other low-level languages for stuff such as fast packet sniffing or the like, and more often that not that is driven by language affinity to te POSIX API as opposed to speed requirements.

To boot pgplex, unlike postgres itself, does not do much heavy lifting as is mostly about bookkeeping of processes and connections; in fact there are no big tight loops and performance is largely dominated by system calls.

I believe Python and its standard modules (many of them we can/do sidestep to save on overhead) generally offer more than enough optimization tools for whatever edge cases we could encounter, and nothing would stop us from writing a custom module if need be.

On the other hand an highly expressive language allows for a far more maintaniable and therefore bug-free implementation, especially in asmall community


---
