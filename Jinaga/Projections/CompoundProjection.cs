using System.Collections.Immutable;
using System.Linq;

namespace Jinaga.Projections
{
    public class CompoundProjection : Projection
    {
        private ImmutableList<(string name, string tag)> fields = ImmutableList<(string name, string tag)>.Empty;

        public CompoundProjection()
        {
        }

        private CompoundProjection(ImmutableList<(string, string)> fields)
        {
            this.fields = fields;
        }

        public CompoundProjection With(string name, string tag)
        {
            return new CompoundProjection(fields.Add((name, tag)));
        }

        public string GetTag(string name)
        {
            return fields
                .Where(field => field.name == name)
                .Select(field => field.tag)
                .Single();
        }

        public override string ToDescriptiveString()
        {
            var fieldString = string.Join("", fields.Select(field => $"        {field.name} = {field.tag}\r\n"));
            return $"{{\r\n{fieldString}    }}";
        }
    }
}