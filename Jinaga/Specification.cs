using System;
using System.Linq;
using Jinaga.Repository;

namespace Jinaga
{
    public static class Given<TFact>
    {
        public static Specification<TFact, TProjection> Match<TProjection>(Func<TFact, FactRepository, IQueryable<TProjection>> spec)
        {
            throw new NotImplementedException();
        }
    }
    public class Specification<TFact, TProjection>
    {

    }
}
