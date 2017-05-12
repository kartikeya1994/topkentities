// split collated file onto different lines
// {
split($0,a," = ");
c = a[2];
n = split(a[1],b,","); 
for(i=1;i<=n;i++)
{
	print b[i] " = " c;
}
}