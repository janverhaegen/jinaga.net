using System.Linq;
using System.Collections.Generic;
using Jinaga.Projections;

namespace Jinaga.Pipelines
{
    class Inverter
    {
        public static IEnumerable<Inverse> InvertSpecification(Specification specification)
        {
            var inverses =
                from start in specification.Pipeline.Starts
                from path in specification.Pipeline.Paths
                where path.Start == start
                from inverse in InvertPaths(start, specification.Pipeline, path, specification.Projection, Pipeline.Empty)
                select inverse;
            return inverses;
        }

        public static IEnumerable<Inverse> InvertPipeline(Pipeline pipeline)
        {
            var inverses =
                from start in pipeline.Starts
                from path in pipeline.Paths
                where path.Start == start
                from inverse in InvertPaths(start, pipeline, path, new EmptyProjection(), Pipeline.Empty)
                select inverse;
            return inverses;
        }

        public static IEnumerable<Inverse> InvertPaths(Label start, Pipeline pipeline, Path path, Projection projection, Pipeline backward)
        {
            var nextBackward = backward.PrependPath(ReversePath(path));
            var conditionalInverses =
                from conditional in pipeline.Conditionals
                where conditional.Start == path.Target
                from inverse in InvertConditionals(conditional, nextBackward, start.Name)
                select inverse;
            var nestedInverses =
                from namedSpecification in projection.GetNamedSpecifications()
                let nestedSpecification = namedSpecification.specification
                from nestedPath in nestedSpecification.Pipeline.Paths
                where nestedPath.Start == path.Target
                from nestedInverse in InvertPaths(start, nestedSpecification.Pipeline, nestedPath, nestedSpecification.Projection, nextBackward)
                select nestedInverse;
            var inversePipeline = nextBackward.AddStart(path.Target);
            var newInverse = new Inverse(
                    inversePipeline,
                    start.Name,
                    Operation.Add,
                    Subset.FromPipeline(inversePipeline));
            var nextInverses =
                from nextPath in pipeline.Paths
                where nextPath.Start == path.Target
                from nextInverse in InvertPaths(start, pipeline, nextPath, projection, nextBackward)
                select nextInverse;
            return nextInverses.Concat(conditionalInverses).Concat(nestedInverses).Prepend(newInverse);
        }

        public static IEnumerable<Inverse> InvertConditionals(Conditional conditional, Pipeline backward, string affectedTag)
        {
            return InvertPipeline(conditional.ChildPipeline)
                .Select(childInverse => new Inverse(
                    childInverse.InversePipeline.Compose(backward),
                    affectedTag,
                    conditional.Exists ? Operation.Add : Operation.Remove,
                    Subset.FromPipeline(backward)));
        }

        public static Path ReversePath(Path path)
        {
            var reverse = new Path(path.Target, path.Start);
            var type = path.Start.Type;
            foreach (var step in path.PredecessorSteps)
            {
                reverse = reverse.PrependSuccessorStep(new Step(step.Role, type));
                type = step.TargetType;
            }
            foreach (var step in path.SuccessorSteps)
            {
                reverse = reverse.PrependPredecessorStep(new Step(step.Role, type));
                type = step.TargetType;
            }
            return reverse;
        }
    }
}