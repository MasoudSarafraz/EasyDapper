using Xunit;

namespace EasyDapper.Tests
{
    public class SimpleTest
    {
        [Fact]
        public void Simple_Test_Should_Pass()
        {
            Assert.True(true);
        }

        [Fact]
        public void Simple_Test_With_Values()
        {
            int a = 5;
            int b = 3;
            Assert.Equal(8, a + b);
        }
    }
}