using System;
using System.Collections.Immutable;

namespace Jinaga.Projections
{
    public abstract class Projection
    {
        public Type Type { get; }

        protected Projection(Type type)
        {
            Type = type;
        }

        public abstract string ToDescriptiveString(int depth = 0);

        public abstract Projection Apply(ImmutableDictionary<string, string> replacements);
        
        public abstract bool CanRunOnGraph { get; }
    }
}