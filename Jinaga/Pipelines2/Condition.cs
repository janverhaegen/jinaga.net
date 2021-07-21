﻿using Jinaga.Visualizers;
using System;

namespace Jinaga.Pipelines2
{
    public class Condition
    {
        private readonly Label start;
        private readonly bool exists;
        private readonly Pipeline childPipeline;

        public Condition(Label start, bool exists, Pipeline childPipeline)
        {
            this.start = start;
            this.exists = exists;
            this.childPipeline = childPipeline;
        }

        public Label Start => start;
        public bool Exists => exists;
        public Pipeline ChildPipeline => childPipeline;

        public string ToDescriptiveString(int depth = 0)
        {
            string op = exists ? "E" : "N";
            string indent = Strings.Indent(depth);
            string child = childPipeline.ToDescriptiveString(depth + 1);
            return $"{indent}{op}(\n{child}{indent})\n";
        }

        public override string ToString()
        {
            return ToDescriptiveString();
        }
    }
}
