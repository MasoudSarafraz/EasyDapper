using System;

namespace EasyDapper.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class IdentityAttribute : Attribute { }
}