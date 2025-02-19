using System;

namespace EasyDapper.Attributes
{
    [AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = true)]
    public class PrimaryKeyAttribute : Attribute
    {
        
    }
}