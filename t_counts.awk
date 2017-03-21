// {
split($0,a," = ");
n = split(a[1],b,","); 
for(i=1;i<=n;i++)
{
	print b[i];
}
}